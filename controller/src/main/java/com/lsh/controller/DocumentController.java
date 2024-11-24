package com.lsh.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.lsh.service.QueryProcessor;
import com.lsh.service.WorkflowManager;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

@Component
@RestController
@RequestMapping("/api/documents")
public class DocumentController {
    private static final Logger logger = LoggerFactory.getLogger(DocumentController.class);

    private int vectorLength = 768;
    private int rows = 20;
    private int bands = 10;
    private String hdfsBasePath = "hdfs://localhost:9000/lsh";
    private String hbaseTableName = "documents";

    @PostMapping("/index")
    public ResponseEntity<String> indexDocuments(@RequestParam("file") MultipartFile file) {
        String jobId = UUID.randomUUID().toString();
        try {
            // Create temp file
            File tempInputFile = File.createTempFile("input_" + jobId, ".json");
            file.transferTo(tempInputFile);

            // Prepare WorkflowManager args    
            String[] workflowArgs = {
                String.valueOf(vectorLength),
                String.valueOf(rows),
                String.valueOf(bands),
                hdfsBasePath + "/random_vectors",
                tempInputFile.getAbsolutePath(),
                hdfsBasePath + "/signatures_" + jobId,
                hdfsBasePath + "/lsh_" + jobId,
                hdfsBasePath + "/conf",
                hbaseTableName
            };

            // Execute WorkflowManager async
            new Thread(() -> {
                try {
                    WorkflowManager.main(workflowArgs);
                    tempInputFile.delete();
                } catch (Exception e) {
                    logger.error("Index job failed, jobId: " + jobId, e);
                }
            }).start();

            return ResponseEntity.ok()
                .body("Index job started, jobId: " + jobId);

        } catch (Exception e) {
            logger.error("Failed to process index request", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Request failed: " + e.getMessage());
        }
    }

    @PostMapping("/query") 
    public ResponseEntity<?> queryDocuments(@RequestParam("file") MultipartFile file) {
        String queryId = UUID.randomUUID().toString();
        try {
            // Create temp files
            File tempQueryFile = File.createTempFile("query_" + queryId, ".json");
            File tempOutputFile = File.createTempFile("result_" + queryId, ".json");
            file.transferTo(tempQueryFile);

            // Prepare QueryProcessor args
            String[] queryArgs = {
                tempQueryFile.getAbsolutePath(),
                hdfsBasePath + "/temp_signatures_" + queryId,
                tempOutputFile.getAbsolutePath(),
                hdfsBasePath + "/random_vectors", 
                hdfsBasePath + "/conf",
                hbaseTableName
            };

            // Execute query
            QueryProcessor.main(queryArgs);

            // Read results
            String result = new String(Files.readAllBytes(tempOutputFile.toPath()), StandardCharsets.UTF_8);

            // Cleanup
            tempQueryFile.delete();
            tempOutputFile.delete();

            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(result);

        } catch (Exception e) {
            logger.error("Failed to process query request", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Query failed: " + e.getMessage());
        }
    }

    //TODO: Job status check to be implemented
    @GetMapping("/status/{jobId}")
    public ResponseEntity<String> getJobStatus(@PathVariable String jobId) {
        try {
            // Job status check logic to be implemented
            // Can check HDFS marker files or other methods
            return ResponseEntity.ok("Job status check not implemented");
        } catch (Exception e) {
            logger.error("Failed to check job status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Status check failed: " + e.getMessage());
        }
    }
}