package com.lsh.service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Random;

public class RandomVectorGenerator {
    public static void main(String[] args) throws Exception {
        // Parameters: n (vector length), r (rows), b (bands)
        int n = Integer.parseInt(args[0]); // vector length
        int r = Integer.parseInt(args[1]); // rows
        int b = Integer.parseInt(args[2]); // bands
        String hdfsPath = args[3]; // HDFS target path

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", 
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);
        Path path = new Path(hdfsPath);

        // Delete if file exists
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        // Create HDFS file
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path)))) {
            Random random = new Random();
            for (int i = 0; i < r * b; i++) {
                StringBuilder vector = new StringBuilder();
                for (int j = 0; j < n; j++) {
                    // Generate random vector elements as 1 or -1
                    vector.append(random.nextBoolean() ? 1 : -1).append(" ");
                }
                writer.write(vector.toString().trim());
                writer.newLine();
            }
        }
    }
}