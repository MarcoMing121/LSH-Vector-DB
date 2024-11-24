package com.lsh.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.JobContext;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HBaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(HBaseLoader.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: HBaseLoader <input path> <output path> <HBase table name>");
            System.exit(-1);
        }

        Configuration conf = HBaseConfiguration.create();
        
        // Set fixed split size
        long splitSize = 64 * 1024 * 1024; // 64MB
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
        conf.setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);
        
        // Calculate appropriate number of mappers based on file size
        Path inputPath = new Path(args[0]);
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(inputPath);
        int numMappers = (int) Math.ceil((double) fileStatus.getLen() / splitSize);
        
        conf.setInt("mapreduce.job.maps", numMappers);
        conf.setInt("mapreduce.job.running.map.limit", numMappers);
        
        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        String tableName = args[2];

        // Check processing flag
        Path flagPath = new Path(args[0] + ".processed");
        
        if (fs.exists(flagPath)) {
            logger.warn("File {} has already been processed, skipping", args[0]);
            return;
        }

        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        
        Job job = Job.getInstance(conf, "Import JSON to HBase");
        job.setJarByClass(HBaseLoader.class);

        job.setMapperClass(JsonToHBaseMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(JsonArrayInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.setNumReduceTasks(0);

        if (job.waitForCompletion(true)) {
            fs.createNewFile(flagPath);
            logger.info("Processing complete, created flag file: {}", flagPath);
        } else {
            throw new Exception("HBase job failed.");
        }
    }

    public static class JsonToHBaseMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        private static final byte[] FAMILY = Bytes.toBytes("data");
        private static final byte[] COL_CONTENT = Bytes.toBytes("content");
        private static final byte[] COL_EMBEDDING = Bytes.toBytes("embedding");
        private static final byte[] COL_METADATA = Bytes.toBytes("metadata");
        private static final byte[] COL_NEED_SIGNATURE = Bytes.toBytes("needSignature");
        
        private List<Put> putList = new ArrayList<>();
        private static final int BATCH_SIZE = 100;
        private Table table;
        private long processedCount = 0;
        private static final Connection connection;
        private static final int LOG_INTERVAL = 1000;
    
        static {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost:2181");
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create HBase connection", e);
            }
        }
    
        @Override
        protected void setup(Context context) throws IOException {
            String tableName = context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE);
            table = connection.getTable(TableName.valueOf(tableName));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            String splitInfo = ((FileSplit) split).toString();
            
            try {
                JSONObject jsonObject = new JSONObject(value.toString());
                String documentId = jsonObject.getString("id");
                
                if (exists(documentId)) {
                    return;
                }
                
                Put put = new Put(Bytes.toBytes(documentId));
                
                if (jsonObject.has("content")) {
                    String content = jsonObject.getString("content");
                    put.addColumn(FAMILY, COL_CONTENT, Bytes.toBytes(content));
                }
                
                if (jsonObject.has("embedding")) {
                    String embeddingStr = jsonObject.getJSONArray("embedding").toString();
                    put.addColumn(FAMILY, COL_EMBEDDING, Bytes.toBytes(embeddingStr));
                }
                
                if (jsonObject.has("metadata")) {
                    String metadataStr = jsonObject.getJSONObject("metadata").toString();
                    put.addColumn(FAMILY, COL_METADATA, Bytes.toBytes(metadataStr));
                }
                
                put.addColumn(FAMILY, COL_NEED_SIGNATURE, Bytes.toBytes("false"));
                
                putList.add(put);
                processedCount++;
                
                if (putList.size() >= BATCH_SIZE) {
                    table.put(putList);
                    putList.clear();
                    context.progress();
                }
            } catch (Exception e) {
                logger.error("Error processing record in {}: {}", splitInfo, e.getMessage(), e);
                throw e;
            }
        }
        
        private boolean exists(String documentId) throws IOException {
            return table.exists(new Get(Bytes.toBytes(documentId)));
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                if (!putList.isEmpty()) {
                    table.put(putList);
                }
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (Exception e) {
                        logger.error("Error closing table", e);
                    }
                }
            }
        }
    }

    public static class JsonArrayInputFormat extends FileInputFormat<LongWritable, Text> {
        private static final Logger logger = LoggerFactory.getLogger(JsonArrayInputFormat.class);

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return true;
        }

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new JsonArrayRecordReader();
        }

        @Override
        protected List<FileStatus> listStatus(JobContext job) throws IOException {
            List<FileStatus> files = super.listStatus(job);
            return files;
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> splits = new ArrayList<>();
            List<FileStatus> files = listStatus(job);
            
            for (FileStatus file : files) {
                Path path = file.getPath();
                long length = file.getLen();
                long splitSize = job.getConfiguration().getLong(
                    "mapreduce.input.fileinputformat.split.maxsize", 67108864);
                
                int numSplits = (int) Math.ceil((double) length / splitSize);
                
                for (int i = 0; i < numSplits; i++) {
                    long splitStart = i * splitSize;
                    long splitLength = Math.min(splitSize, length - splitStart);
                    
                    if (splitLength <= 0) break;
                    
                    FileSplit split = new FileSplit(path, splitStart, splitLength, null);
                    splits.add(split);
                }
            }
            
            return splits;
        }
    }

    public static class JsonArrayRecordReader extends RecordReader<LongWritable, Text> {
        private static final Logger logger = LoggerFactory.getLogger(JsonArrayRecordReader.class);
        private BufferedReader reader;
        private long currentPos;
        private Text currentValue = new Text();
        private FileSystem fs;
        private long start;
        private long end;
        private String splitInfo;
        private boolean stillInSplit = true;
        
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit) split;
            Path path = fileSplit.getPath();
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            
            splitInfo = String.format("%s [%d:%d]", path.getName(), start, end);
            
            fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream fileIn = fs.open(path);
            
            if (start != 0) {
                long lookBehind = Math.min(64 * 1024, start);
                long seekPos = start - lookBehind;
                fileIn.seek(seekPos);
                
                StringBuilder buffer = new StringBuilder();
                int brackets = 0;
                boolean foundStart = false;
                
                while (true) {
                    int c = fileIn.read();
                    if (c == -1) break;
                    
                    char ch = (char) c;
                    if (brackets == 0 && ch == '{') {
                        foundStart = true;
                        currentPos = fileIn.getPos() - 1;
                        fileIn.seek(currentPos);
                        break;
                    }
                    
                    if (ch == '{') brackets++;
                    if (ch == '}') brackets--;
                }
                
                if (!foundStart) {
                    logger.error("Cannot find valid JSON object start in {}", splitInfo);
                    throw new IOException("Invalid JSON format");
                }
            } else {
                int c;
                while ((c = fileIn.read()) != -1) {
                    if (c == '[') {
                        break;
                    }
                }
            }
            
            reader = new BufferedReader(new InputStreamReader(fileIn));
        }
        
        @Override
        public boolean nextKeyValue() throws IOException {
            if (!stillInSplit) {
                return false;
            }
            
            StringBuilder jsonObject = new StringBuilder();
            int brackets = 0;
            boolean inObject = false;
            
            int c;
            while ((c = reader.read()) != -1) {
                currentPos++;
                char ch = (char) c;
                
                if (ch == '{') {
                    if (!inObject) {
                        inObject = true;
                    }
                    brackets++;
                    jsonObject.append(ch);
                } else if (inObject) {
                    jsonObject.append(ch);
                    if (ch == '{') {
                        brackets++;
                    } else if (ch == '}') {
                        brackets--;
                        if (brackets == 0) {
                            String json = jsonObject.toString();
                            
                            try {
                                JSONObject obj = new JSONObject(json);
                                if (!obj.has("id")) {
                                    return nextKeyValue();
                                }
                                currentValue.set(json);
                                
                                if (currentPos > end) {
                                    stillInSplit = false;
                                }
                                return true;
                                
                            } catch (Exception e) {
                                return nextKeyValue();
                            }
                        }
                    }
                }
                
                if (ch == ']' && !inObject) {
                    stillInSplit = false;
                    return false;
                }
            }
            
            stillInSplit = false;
            return false;
        }
        
        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(currentPos);
        }
        
        @Override
        public Text getCurrentValue() {
            return currentValue;
        }
        
        @Override
        public float getProgress() {
            if (end == start) {
                return 0.0f;
            }
            return Math.min(1.0f, (currentPos - start) / (float)(end - start));
        }
        
        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
