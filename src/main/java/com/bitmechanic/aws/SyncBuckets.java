package com.bitmechanic.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by James Cooper <james@bitmechanic.com>
 * Date: Apr 13, 2010
 */
public class SyncBuckets {

    public static void main(String argv[]) throws Exception {
        String from = null;
        String to = null;
        boolean verbose = false;
        for (int i = 0; i < argv.length; i++) {
            if (argv[i].equals("--from"))
                from = argv[++i];
            else if (argv[i].equals("--to"))
                to = argv[++i];
            else if (argv[i].equals("--verbose"))
                verbose = true;
        }

        if (from == null || to == null)
            usage();

        SyncBuckets sb = new SyncBuckets(from, to, verbose);
        sb.run();
    }

    private static void usage() {
        System.err.println("Usage: java SyncBuckets [--verbose] --from accessKey:secretKey:bucketName --to accessKey:secretKey:bucketName");
        System.exit(1);
    }

    ////////

    private S3Config fromS3;
    private S3Config toS3;
    private boolean verbose;

    public SyncBuckets(String from, String to, boolean verbose) {
        this.fromS3  = createS3(from);
        this.toS3    = createS3(to);
        this.verbose = verbose;
    }

    public void run() throws Exception {

        long start = System.currentTimeMillis();
        long copyCount = 0;
        long deleteCount = 0;

        try {
            toS3.client.getBucketLocation(toS3.bucket);
        }
        catch (AmazonServiceException e) {
            if (e.getErrorCode().equals("NoSuchBucket")) {
                toS3.client.createBucket(toS3.bucket);
            }
            else {
                throw e;
            }
        }

        Map<String,String> keysToDelete = new HashMap<String,String>();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(toS3.bucket);
        ObjectListing listing = null;
        do {
            if (listing != null)
                request.setMarker(listing.getNextMarker());

            listing = toS3.client.listObjects(request);
            for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                keysToDelete.put(summary.getKey(), summary.getETag());
            }
        }
        while (listing.isTruncated());

        if (verbose) {
            System.out.println("Objects in destination: " + keysToDelete.size());
        }

        TaskQueue queue = new TaskQueue();
        for (int i = 0; i < 10; i++) {
            WorkerThread t = new WorkerThread(queue);
            t.start();
        }

        request = new ListObjectsRequest();
        request.setBucketName(fromS3.bucket);
        listing = null;
        do {
            if (listing != null)
                request.setMarker(listing.getNextMarker());

            listing = fromS3.client.listObjects(request);
            for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                String toEtag = keysToDelete.get(summary.getKey());
                if (toEtag == null || !summary.getETag().equals(toEtag)) {
                    queue.enqueue(new CopyTask(summary.getKey()));
                    copyCount++;
                }
                keysToDelete.remove(summary.getKey());
            }
        }
        while (listing.isTruncated());

        deleteCount = keysToDelete.size();
        for (String key : keysToDelete.keySet()) {
            queue.enqueue(new DeleteTask(key));
        }

        while (queue.size() > 0) {
            Thread.sleep(100);
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Elapsed time: " + (elapsed / 1000) + " seconds");
        System.out.println("      Copied: " + copyCount);
        System.out.println("     Deleted: " + deleteCount);
    }

    private void copyObject(String key) throws IOException {
        if (verbose)
            System.out.println("Copying: " + key);
        S3Object object = fromS3.client.getObject(fromS3.bucket, key);
        toS3.client.putObject(toS3.bucket, key, object.getObjectContent(), object.getObjectMetadata());
        object.getObjectContent().close();
    }

    private S3Config createS3(String s3Creds) {
        String parts[] = s3Creds.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid s3 creds: " + s3Creds + " should be  accessKey:secretKey:bucket");
        }
        else {
            AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(parts[0], parts[1]));
            S3Config config = new S3Config();
            config.client = s3;
            config.bucket = parts[2];
            return config;
        }
    }

    class S3Config {
        AmazonS3Client client;
        String bucket;
    }

    abstract class BaseTask implements Runnable {
        
        String key;

        BaseTask(String key) {
            this.key = key;
        }

        @Override
        public void run() {
            Exception ex = null;
            int retry = 0;
            boolean success = false;
            while (retry < 3 && !success) {
                try {
                    execTask();
                    success = true;
                }
                catch (Exception e) {
                    ex = e;
                    retry++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            if (!success && ex != null) {
                ex.printStackTrace();
            }
        }

        abstract void execTask() throws Exception;
    }

    class DeleteTask extends BaseTask {

        DeleteTask(String key) {
            super(key);
        }

        @Override
        void execTask() throws Exception {
            toS3.client.deleteObject(toS3.bucket, key);
        }
    }

    class CopyTask extends BaseTask {

        CopyTask(String key) {
            super(key);
        }

        @Override
        void execTask() throws Exception {
            copyObject(key);
        }
    }

    class WorkerThread extends Thread {

        TaskQueue queue;

        WorkerThread(TaskQueue queue) {
            setDaemon(true);
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true) {
                Runnable r = null;
                try {
                    r = queue.dequeue();
                    r.run();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class TaskQueue {

        List<Runnable> runnableList = new ArrayList<Runnable>();

        public synchronized Runnable dequeue() throws InterruptedException {
            while (runnableList.isEmpty())
                wait();
            return runnableList.remove(runnableList.size()-1);
        }

        public synchronized void enqueue(Runnable runnable) {
            runnableList.add(runnable);
            notifyAll();
        }

        public synchronized int size() {
            return runnableList.size();
        }

    }

}
