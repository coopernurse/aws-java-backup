package com.bitmechanic.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.google.gson.Gson;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by James Cooper <james@bitmechanic.com>
 * Date: Apr 13, 2010
 */
public class ImportExport {

    public static void main(String argv[]) throws Exception {
        String accessKey = null;
        String secretKey = null;
        String bucket = null;
        String domain = null;
        String importFile = null;
        String exportFile = null;
        for (int i = 0; i < argv.length; i++) {
            if (argv[i].equals("--accessKey"))
                accessKey = argv[++i];
            else if (argv[i].equals("--secretKey"))
                secretKey = argv[++i];
            else if (argv[i].equals("--bucket"))
                bucket = argv[++i];
            else if (argv[i].equals("--domain"))
                domain = argv[++i];
            else if (argv[i].equals("--import"))
                importFile = argv[++i];
            else if (argv[i].equals("--export"))
                exportFile = argv[++i];
        }

        ImportExport importExport = new ImportExport(accessKey, secretKey, bucket, domain);
        if (importFile != null) {
            importExport.importData(importFile);
        }
        else if (exportFile != null) {
            importExport.exportData(exportFile);
        }
        else {
            throw new Exception("--export or --import is required");
        }
    }

    ////////////////////////////////////////

    BasicAWSCredentials creds;
    AmazonSimpleDB simpleDb;
    AmazonS3 s3;
    String bucket;
    String domain;

    Gson gson;

    List<String> idList;

    public ImportExport(String accessKey, String secretKey, String bucket, String domain) {
        creds = new BasicAWSCredentials(accessKey, secretKey);
        simpleDb = new AmazonSimpleDBClient(creds);
        s3 = new AmazonS3Client(creds);

        this.bucket = bucket;
        this.domain = domain;
        this.gson = new Gson();
        System.out.println("Bucket: " + bucket + " domain: " + domain);
    }

    public void exportData(String directory) throws IOException, InterruptedException {
        File dir = new File(directory);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("Not a directory: " + directory);
        }

        System.out.println("Exporting data to dir: " + directory);

        this.idList = new ArrayList<String>();
        exportSimpledb(directory);

        if (bucket != null)
            exportS3Objects(directory);
    }

    private void exportS3Objects(String directory) throws IOException, InterruptedException {
        final File dir = new File(directory + File.separatorChar + "objects");
        dir.mkdirs();

        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("Unable to create: " + dir.getAbsolutePath());
        }

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(new Runnable() {
                public void run() {
                    String id = null;
                    do {
                        try {
                            id = exportNextId(dir);
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    while (id != null);
                }
            });
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private synchronized String popIdToExport() {
        if (idList.isEmpty())
            return null;
        else
            return idList.remove(idList.size()-1);
    }

    private String exportNextId(File dir) throws IOException {
        String id = popIdToExport();
        if (id != null) {
            int count = 0;

            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(id);
            ObjectListing listing = null;
            do {
                if (listing != null)
                    request.setMarker(listing.getNextMarker());

                listing = s3.listObjects(request);
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    count++;
                    String filename = dir.getAbsolutePath() + File.separatorChar + id + "_" + count;
                    getObject(filename, summary.getKey());
                }
            }
            while (listing.isTruncated());
        }
        return id;
    }

    private void getObject(String filename, String key) throws IOException {
        System.out.println("Saving " + key + " to " + filename);
        
        S3Object object = s3.getObject(bucket, key);

        FileOutputStream fos = new FileOutputStream(filename);
        InputStream is = object.getObjectContent();
        byte arr[] = new byte[4096];
        int length;
        while ((length = is.read(arr)) != -1) {
            fos.write(arr, 0, length);
        }
        is.close();
        fos.close();

        PrintWriter writer = new PrintWriter(new FileWriter(filename + "_key"));
        writer.println(key);
        writer.close();

        writer = new PrintWriter(new FileWriter(filename + "_meta"));
        writer.println(gson.toJson(object.getObjectMetadata()));
        writer.close();
    }

    private void exportSimpledb(String directory) throws IOException {
        String file = directory + File.separatorChar + "simpledb.txt";
        PrintWriter writer = new PrintWriter(new FileWriter(file));

        writer.print("[");
        int itemCount = 0;
        long start = System.currentTimeMillis();

        String query = "select * from " + domain + " limit 100";
        SelectRequest request = new SelectRequest(query);
        SelectResult result = null;
        do {
            if (result != null)
                request.setNextToken(result.getNextToken());
            
            result = simpleDb.select(request);
            List<Item> items = result.getItems();
            if (items != null) {
                for (Item item : items) {
                    if (itemCount > 0) {
                        writer.print(",");
                    }
                    String json = gson.toJson(item);
                    writer.println(json);
                    itemCount++;
                    idList.add(item.getName());
                }
            }
        }
        while (result.getNextToken() != null && result.getNextToken().length() > 0);

        writer.println("]");
        writer.flush();
        writer.close();

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Exported " + itemCount + " items in " + (elapsed/1000) + " seconds");
    }


    ///////////////////////////

    public void importData(String file) {
        
    }

}
