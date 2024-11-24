package com.lsh.service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class WorkflowManager {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", 
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);

        // Parameters: n (vector length), r (rows), b (bands), hdfsPath (HDFS target path)
        int n = Integer.parseInt(args[0]); // vector length
        int r = Integer.parseInt(args[1]); // rows
        int b = Integer.parseInt(args[2]); // bands
        String randomVectorPath = args[3]; // Random vector file HDFS path
        String jsonInputPath = args[4]; // JSON input file HDFS path
        String signatureOutputPath = args[5]; // Signature output path
        String lshOutputPath = args[6]; // LSH output path
        String confPath = args[7]; // Config file path
        String hbaseTableName = args[8]; // HBase table name

        // Create necessary tables before processing
        createHBaseTables(hbaseTableName);

        // Write rows and bands to config file
        writeConfig(confPath, r, b, fs);

        // Check if random vector file exists
        Path randomVectorFilePath = new Path(randomVectorPath);
        if (!fs.exists(randomVectorFilePath)) {
            // Generate random vectors if not exists
            String[] randomVectorArgs = {String.valueOf(n), String.valueOf(r), String.valueOf(b), randomVectorPath};
            RandomVectorGenerator.main(randomVectorArgs);
        }

        // Check if number of random vectors is sufficient
        int existingVectorsCount = (int) fs.listStatus(randomVectorFilePath.getParent()).length;
        if (existingVectorsCount < r * b) {
            String[] randomVectorArgs = {String.valueOf(n), String.valueOf(r), String.valueOf(b), randomVectorPath};
            RandomVectorGenerator.main(randomVectorArgs);
        }

        // Run HBaseLoader to load JSON data into HBase
        String[] hbaseLoaderArgs = {jsonInputPath, "/tmp/hbaseloader_output", hbaseTableName};
        HBaseLoader.main(hbaseLoaderArgs);
        
        // Run SignatureCalculator to read from HBase and generate signatures
        String[] signatureCalculatorArgs = {
            "/tmp/dummy_input",
            signatureOutputPath,
            randomVectorPath,
            hbaseTableName
        };
        SignatureCalculator.main(signatureCalculatorArgs);

        // Run LSH to read signatures and store results in HBase
        String[] lshArgs = {
            signatureOutputPath,
            "/tmp/lsh_output",
            String.valueOf(b),
            String.valueOf(r),
            hbaseTableName + "_bucket"
        };
        LSH.main(lshArgs);

        // Clean up temp directories
        fs.delete(new Path("/tmp/hbaseloader_output"), true);
        fs.delete(new Path("/tmp/lsh_output"), true);
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Execution time: " + executionTime + " milliseconds");
    }

    private static void writeConfig(String confPath, int r, int b, FileSystem fs) throws Exception {
        Path configPath = new Path(confPath);
        if (fs.exists(configPath)) {
            fs.delete(configPath, true);
        }

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(configPath)))) {
            writer.write("bands=" + b + "\n");
            writer.write("rows=" + r + "\n");
        }
    }

    private static void createHBaseTables(String baseTableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            
            // Create main document table
            TableName documentsTable = TableName.valueOf(baseTableName);
            if (!admin.tableExists(documentsTable)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(documentsTable)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build())
                        .build();
                admin.createTable(tableDescriptor);
            }

            // Create bucket table
            TableName bucketsTable = TableName.valueOf(baseTableName + "_bucket");
            if (!admin.tableExists(bucketsTable)) {
                TableDescriptor bucketTableDescriptor = TableDescriptorBuilder.newBuilder(bucketsTable)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build())
                        .build();
                admin.createTable(bucketTableDescriptor);
            }
        } catch (IOException e) {
            throw e;
        }
    }
}
