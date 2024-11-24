package com.lsh.service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryProcessor {
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    private static final String DATA_FAMILY = "data";
    private static final String DOCUMENTS_COLUMN = "documents";
    private static final String CONTENT_COLUMN = "content";
    private static final String EMBEDDING_COLUMN = "embedding";
    private static final String METADATA_COLUMN = "metadata";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            if (args.length != 6) {
                throw new IllegalArgumentException(
                    "Usage: QueryProcessor <query JSON file> <temp signature path> <result JSON path> " +
                    "<random vector path> <config path> <HBase table name>"
                );
            }

            String queryJsonPath = args[0];
            String tempSignaturePath = args[1];
            String outputJsonPath = args[2];
            String randomVectorPath = args[3];
            String confPath = args[4];
            String hbaseTableName = args[5];

            Configuration conf = HBaseConfiguration.create();
            conf.set("fs.hdfs.impl", 
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);

            String tempHbaseTable = "temp_query_" + System.currentTimeMillis();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                TableName documentsTable = TableName.valueOf(tempHbaseTable);
                if (!admin.tableExists(documentsTable)) {
                    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(documentsTable)
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build())
                            .build();
                    admin.createTable(tableDescriptor);
                }

                TableName bucketsTable = TableName.valueOf(tempHbaseTable + "_bucket");
                if (!admin.tableExists(bucketsTable)) {
                    TableDescriptor bucketTableDescriptor = TableDescriptorBuilder.newBuilder(bucketsTable)
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build())
                            .build();
                    admin.createTable(bucketTableDescriptor);
                }
            }

            try {
                String[] hbaseLoaderArgs = {queryJsonPath, "/tmp/query_loader_output", tempHbaseTable};
                HBaseLoader.main(hbaseLoaderArgs);

                String[] signatureCalculatorArgs = {
                    "/tmp/dummy_input",
                    tempSignaturePath,
                    randomVectorPath,
                    tempHbaseTable
                };
                SignatureCalculator.main(signatureCalculatorArgs);

                int[] lshParams = readLSHParams(confPath, conf);
                int bands = lshParams[0];
                int rows = lshParams[1];

                String[] lshArgs = {
                    tempSignaturePath,
                    "/tmp/query_lsh_output",
                    String.valueOf(bands),
                    String.valueOf(rows),
                    tempHbaseTable + "_bucket"
                };
                LSH.main(lshArgs);

                Set<String> similarDocIds = findSimilarDocuments(
                    tempHbaseTable + "_bucket",
                    hbaseTableName + "_bucket",
                    conf
                );

                writeResults(similarDocIds, hbaseTableName, outputJsonPath, conf);

            } catch (Exception e) {
                logger.error("Error processing query", e);
                throw e;
            } finally {
                cleanupTemporaryResources(fs, conf, tempHbaseTable);
            }
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            System.out.println("Execution time: " + executionTime + " milliseconds");

        } catch (Exception e) {
            logger.error("Program execution failed", e);
            System.exit(1);
        }
    }

    private static Set<String> findSimilarDocuments(String queryBucketTable, 
                                                   String dataBucketTable,
                                                   Configuration conf) throws Exception {
        Set<String> similarDocIds = new HashSet<>();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table queryTable = connection.getTable(TableName.valueOf(queryBucketTable));
            Table dataTable = connection.getTable(TableName.valueOf(dataBucketTable));

            try {
                List<Result> queryBuckets = getAllBuckets(queryTable);
                
                for (Result queryBucket : queryBuckets) {
                    String bucketId = Bytes.toString(queryBucket.getRow());
                    Result dataBucket = dataTable.get(new Get(Bytes.toBytes(bucketId)));
                    
                    if (!dataBucket.isEmpty()) {
                        byte[] docs = dataBucket.getValue(
                            Bytes.toBytes(DATA_FAMILY), 
                            Bytes.toBytes(DOCUMENTS_COLUMN)
                        );
                        if (docs != null) {
                            String[] docIds = Bytes.toString(docs).split(",");
                            for (String docId : docIds) {
                                similarDocIds.add(docId.trim());
                            }
                        }
                    }
                }
            } finally {
                queryTable.close();
                dataTable.close();
            }
        }
        return similarDocIds;
    }

    private static void writeResults(Set<String> documentIds, 
                                   String sourceTableName,
                                   String outputPath,
                                   Configuration conf) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table sourceTable = connection.getTable(TableName.valueOf(sourceTableName))) {
            
            JSONArray results = new JSONArray();
            int processedDocs = 0;
            
            for (String docId : documentIds) {
                Result result = sourceTable.get(new Get(Bytes.toBytes(docId)));
                
                if (!result.isEmpty()) {
                    JSONObject doc = new JSONObject();
                    doc.put("id", docId);
                    doc.put("content", Bytes.toString(result.getValue(
                        Bytes.toBytes(DATA_FAMILY), 
                        Bytes.toBytes(CONTENT_COLUMN)
                    )));
                    doc.put("metadata", new JSONObject(Bytes.toString(
                        result.getValue(Bytes.toBytes(DATA_FAMILY), 
                        Bytes.toBytes(METADATA_COLUMN))
                    )));
                    results.put(doc);
                    
                    processedDocs++;
                }
            }
            FileSystem fs = FileSystem.get(conf);
            Path outputFilePath = new Path(outputPath);
            if (fs.exists(outputFilePath)) {
                fs.delete(outputFilePath, true);
            }

            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(outputFilePath)))) {
                writer.write(results.toString(2));
            }
        }
    }

    private static void cleanupTemporaryResources(FileSystem fs, 
                                                Configuration conf,
                                                String tempTableName) throws Exception {
        try {
            fs.delete(new Path("/tmp/query_loader_output"), true);
            fs.delete(new Path("/tmp/query_lsh_output"), true);

            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                
                TableName tableName = TableName.valueOf(tempTableName);
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
                
                TableName bucketTableName = TableName.valueOf(tempTableName + "_bucket");
                if (admin.tableExists(bucketTableName)) {
                    admin.disableTable(bucketTableName);
                    admin.deleteTable(bucketTableName);
                }
            }
        } catch (Exception e) {
            logger.error("Error cleaning up temporary resources", e);
            throw e;
        }
    }

    private static List<Result> getAllBuckets(Table table) throws Exception {
        List<Result> results = new ArrayList<>();
        try (ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                results.add(result);
            }
        }
        return results;
    }

    private static int[] readLSHParams(String confPath, Configuration conf) throws Exception {
        int[] params = new int[2];
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(new Path(confPath))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    if (parts[0].trim().equals("bands")) {
                        params[0] = Integer.parseInt(parts[1].trim());
                    } else if (parts[0].trim().equals("rows")) {
                        params[1] = Integer.parseInt(parts[1].trim());
                    }
                }
            }
        }
        return params;
    }
}