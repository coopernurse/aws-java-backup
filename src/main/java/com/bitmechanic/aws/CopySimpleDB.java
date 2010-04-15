package com.bitmechanic.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by James Cooper <james@bitmechanic.com>
 * Date: Apr 14, 2010
 */
public class CopySimpleDB {

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

        CopySimpleDB copy = new CopySimpleDB(from, to, verbose);
        copy.run();
    }

    private static void usage() {
        System.err.println("Usage: java CopySimpleDB [--verbose] --from accessKey:secretKey:bucketName --to accessKey:secretKey:bucketName");
        System.exit(1);
    }

    ////////////////////////////////

    SimpleDBConfig fromSimple;
    SimpleDBConfig toSimple;
    boolean verbose;

    public CopySimpleDB(String from, String to, boolean verbose) {
        this.fromSimple  = createSimpleDB(from);
        this.toSimple    = createSimpleDB(to);
        this.verbose     = verbose;
    }

    public void run() throws Exception {

        long start = System.currentTimeMillis();
        long itemCount = 0;

        ListDomainsResult domains = toSimple.client.listDomains();
        if (domains.getDomainNames().contains(toSimple.domain)) {
            System.out.println("Deleting destination domain: " + toSimple.domain);
            toSimple.client.deleteDomain(new DeleteDomainRequest(toSimple.domain));
        }
        System.out.println("Creating destination domain: " + toSimple.domain);
        toSimple.client.createDomain(new CreateDomainRequest(toSimple.domain));

        String query = "select * from " + fromSimple.domain + " limit 20";
        SelectRequest request = new SelectRequest(query);
        request.setConsistentRead(true);
        SelectResult result = null;
        do {
            if (result != null)
                request.setNextToken(result.getNextToken());

            result = fromSimple.client.select(request);
            List<Item> items = result.getItems();
            if (items != null && items.size() > 0) {
                BatchPutAttributesRequest put = new BatchPutAttributesRequest();
                put.setDomainName(toSimple.domain);

                List<ReplaceableItem> repItems = new ArrayList<ReplaceableItem>();
                for (Item item : items) {
                    ReplaceableItem repItem = new ReplaceableItem();
                    repItem.setName(item.getName());

                    List<ReplaceableAttribute> repAttribs = new ArrayList<ReplaceableAttribute>();
                    for (Attribute attrib : item.getAttributes()) {
                        ReplaceableAttribute repAttrib = new ReplaceableAttribute();
                        repAttrib.setName(attrib.getName());
                        repAttrib.setValue(attrib.getValue());
                        repAttribs.add(repAttrib);
                    }
                    repItem.setAttributes(repAttribs);
                    
                    repItems.add(repItem);
                    itemCount++;
                }
                put.setItems(repItems);

                toSimple.client.batchPutAttributes(put);
            }
        }
        while (result.getNextToken() != null && result.getNextToken().length() > 0);

        long elapsed = System.currentTimeMillis() - start;
        
        System.out.println("Items in from domain: " + getCount(fromSimple));
        System.out.println("  Items in to domain: " + getCount(toSimple));

        System.out.println("Elapsed time: " + (elapsed / 1000) + " seconds");
        System.out.println("       Items: " + itemCount);
    }

    private int getCount(SimpleDBConfig config) {
        String query = "select count(*) from " + config.domain;
        SelectRequest request = new SelectRequest(query);
        request.setConsistentRead(true);
        SelectResult result = config.client.select(request);
        return Integer.parseInt(result.getItems().get(0).getAttributes().get(0).getValue());
    }

    private SimpleDBConfig createSimpleDB(String conf) {
        String parts[] = conf.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid SimpleDB creds: " + conf + " should be  accessKey:secretKey:domain");
        }
        else {
            AmazonSimpleDBClient simpleDB = new AmazonSimpleDBClient(new BasicAWSCredentials(parts[0], parts[1]));
            SimpleDBConfig config = new SimpleDBConfig();
            config.client = simpleDB;
            config.domain = parts[2];
            return config;
        }
    }

    class SimpleDBConfig {
        AmazonSimpleDB client;
        String domain;
    }

}
