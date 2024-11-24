package com.lsh.service;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// HBase imports
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

// Java standard imports
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class LSH {
    public static class LSHMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int bands;
        private int rows;

        @Override
        protected void setup(Context context) throws IOException {
            bands = context.getConfiguration().getInt("bands", 5);
            rows = context.getConfiguration().getInt("rows", 2);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;

            String documentId = parts[0];
            String[] signatureParts = parts[1].split(",");

            // Calculate bucket ID for each band
            for (int band = 0; band < bands; band++) {
                int bucketValue = 0;
                for (int row = 0; row < rows; row++) {
                    int index = band * rows + row;
                    if (index < signatureParts.length) {
                        int bit = Integer.parseInt(signatureParts[index]) >= 0 ? 1 : 0;
                        bucketValue = (bucketValue << 1) | bit;
                    }
                }
                String bucketId = band + "_" + bucketValue;
                context.write(new Text(bucketId), new Text(documentId));
            }
        }
    }

    public static class LSHReducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Set<String> uniqueDocIds = new HashSet<>();
            for (Text docId : values) {
                uniqueDocIds.add(docId.toString());
            }

            if (!uniqueDocIds.isEmpty()) {
                Put put = new Put(key.toString().getBytes());
                put.addColumn(Bytes.toBytes("data"), 
                    Bytes.toBytes("documents"), 
                    Bytes.toBytes(String.join(",", uniqueDocIds)));
                context.write(NullWritable.get(), put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: LSH <input path> <output path> <bands> <rows> <HBase table name>");
            System.exit(-1);
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        String tableName = args[4];
        
        conf.setInt("bands", Integer.parseInt(args[2]));
        conf.setInt("rows", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Locality Sensitive Hashing");
        job.setJarByClass(LSH.class);

        // Set Mapper
        job.setMapperClass(LSHMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set TableReducer
        TableMapReduceUtil.initTableReducerJob(
            tableName,        // table name
            LSHReducer.class,    // reducer class
            job,
            null,null,null,null,
            false
        );

        // Set input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            throw new Exception("LSH job failed.");
        }
        System.out.println("LSH processing completed.");
    }
}