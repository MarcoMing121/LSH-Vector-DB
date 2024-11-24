package com.lsh.service;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// HBase imports
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.CompareOperator;

// Java standard imports
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

// JSON handling
import org.json.JSONArray;

public class SignatureCalculator {
    public static class SignatureMapper extends TableMapper<Text, Text> {
        private Connection connection;
        private Table table;
        private List<int[]> randomVectors = new ArrayList<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read random vector file
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),context.getConfiguration());
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFileURI : cacheFiles) {
                    Path path = new Path(cacheFileURI.getPath());
                    Scanner scanner = new Scanner(fs.open(path));
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        String[] elements = line.trim().split(" ");
                        int[] vector = new int[elements.length];
                        for (int i = 0; i < elements.length; i++) {
                            vector[i] = Integer.parseInt(elements[i]);
                        }
                        randomVectors.add(vector);
                    }
                    scanner.close();
                }
            }
            // Initialize HBase connection
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost:2181");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(context.getConfiguration().get("hbase.table.name")));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) 
                throws IOException, InterruptedException {
            // Get document ID (row key)
            String documentId = Bytes.toString(key.get());
            
            // Read embedding column from HBase
            byte[] embeddingBytes = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("embedding"));
            if (embeddingBytes == null) {
                return; // Skip if no embedding data
            }
            String embeddingStr = Bytes.toString(embeddingBytes);
            JSONArray embeddingArray = new JSONArray(embeddingStr);
            double[] embedding = new double[embeddingArray.length()];

            for (int j = 0; j < embeddingArray.length(); j++) {
                embedding[j] = embeddingArray.getDouble(j);
            }

            // Calculate dot product with each random vector
            StringBuilder dotProducts = new StringBuilder();
            for (int[] randomVector : randomVectors) {
                double dotProduct = 0;
                for (int j = 0; j < embedding.length; j++) {
                    dotProduct += embedding[j] * randomVector[j];
                }
                if (dotProduct >= 0.) {
                    dotProducts.append("1,");
                } else {
                    dotProducts.append("-1,");
                }
            }

            // Update needSignature column to true
            Put put = new Put(key.get());
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("needSignature"), Bytes.toBytes("true"));
            table.put(put);

            // Output to HDFS: key is document ID, value is signature
            context.write(new Text(documentId), new Text(dotProducts.toString().trim()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
                if (table != null) {
                try {
                    table.close();
                } finally {
                    table = null;
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } finally {
                    connection = null;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: SignatureCalculator <input path> <output path> <random vector file path> <HBase table name>");
            System.exit(-1);
        }
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181");  
        conf.set("hbase.table.name", args[3]);
        conf.set("fs.hdfs.impl", 
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        Job job = Job.getInstance(conf, "Signature Calculator");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);
        conf.set("fs.defaultFS", fs.getUri().toString());
        job.setJarByClass(SignatureCalculator.class);

        // Modify Scan object to only get unprocessed documents
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("embedding"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("needSignature"));
        
        // Add filter to only process rows where needSignature is false
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            Bytes.toBytes("data"),
            Bytes.toBytes("needSignature"),
            CompareOperator.EQUAL,
            Bytes.toBytes("false")
        );
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

        // Initialize TableMapper
        TableMapReduceUtil.initTableMapperJob(
            args[3],        // table name
            scan,           // Scan instance
            SignatureMapper.class,    // Mapper class
            Text.class,     // Mapper output key type
            Text.class,     // Mapper output value type
            job,
            false
        );

        // Set output format
        job.setNumReduceTasks(0); // No reducer needed
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Add random vector file to distributed cache
        job.addCacheFile(new Path(args[2]).toUri());

        if (!job.waitForCompletion(true)) {
            throw new Exception("SignatureCalculator job failed.");
        }
        System.out.println("Signature calculation completed.");
    }
}