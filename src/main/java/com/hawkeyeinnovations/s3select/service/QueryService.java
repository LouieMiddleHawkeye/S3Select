package com.hawkeyeinnovations.s3select.service;

import com.hawkeyeinnovations.dataconnections.s3.S3Utils;
import com.hawkeyeinnovations.s3select.model.FileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class QueryService {

//    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Region region;

    public QueryService(@Value("${region}") String region) {
        this.region = Region.of(region);
    }

    public void query(String key, String outputPath, String query, String bucket, FileType fileType) {
        long start = System.currentTimeMillis();
        log.info("Starting query");

        if (key == null) {
            key = "";
        }

        S3AsyncClient s3Client = S3AsyncClient.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.builder()
                .profileName("okta")
                .build())
            .build();
        S3Utils s3Utils = new S3Utils(s3Client, bucket, key, region);

        if (!Files.exists(Paths.get(outputPath))) {
            if (!new File(outputPath).mkdirs()) {
                log.error("Failed to create output directory");
                return;
            }
        }

        List<String> files = s3Utils.getFilesWithPrefix(key);
        processFiles(files, key, query, outputPath, s3Client, fileType, bucket);

        log.info("Took " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + "s");
    }

    private void processFiles(List<String> files, String key, String query, String outputPath, S3AsyncClient s3Client,
                              FileType fileType, String bucket) {
        files.forEach(file -> {
            SelectObjectContentRequest request = FileType.CSV.equals(fileType) ?
                generateRequestCSV(bucket, file, query) : generateRequestJSON(bucket, file, query);
            try {
                s3Client.selectObjectContent(request, SelectObjectContentResponseHandler.builder()
                        .subscriber(SelectObjectContentResponseHandler.Visitor.builder()
                            .onRecords(event -> {
                                String outputLocation = outputPath + File.separator + file.replace(key, "");
                                if (!Files.exists(Path.of(outputLocation))) {
                                    log.info("Writing file " + file.replace(key, "") + "...");
                                    try (OutputStream outputStream = new FileOutputStream(outputLocation)) {
                                        outputStream.write(event.payload().asByteArray());
                                    } catch (IOException e) {
                                        log.error("Failed to write file", e);
                                    }
                                }
                            })
                            .build())
                        .build())
                    .get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to run query '{}' on file '{}'", query, file, e);
            }
        });
    }

    public static SelectObjectContentRequest generateRequestCSV(String bucket, String key, String query) {
        InputSerialization inputSerialization = InputSerialization.builder()
            .csv(CSVInput.builder().build())
            .compressionType(CompressionType.NONE)
            .build();

        OutputSerialization outputSerialization = OutputSerialization.builder()
            .csv(CSVOutput.builder().build())
            .build();

        return SelectObjectContentRequest.builder()
            .bucket(bucket)
            .key(key)
            .expression(query)
            .expressionType(ExpressionType.SQL)
            .inputSerialization(inputSerialization)
            .outputSerialization(outputSerialization)
            .build();
    }

    public static SelectObjectContentRequest generateRequestJSON(String bucket, String key, String query) {
        InputSerialization inputSerialization = InputSerialization.builder()
            .json(JSONInput.builder()
                .type(JSONType.DOCUMENT)
                .build())
            .compressionType(CompressionType.NONE)
            .build();

        OutputSerialization outputSerialization = OutputSerialization.builder()
            .json(JSONOutput.builder().build())
            .build();

        return SelectObjectContentRequest.builder()
            .bucket(bucket)
            .key(key)
            .expression(query)
            .expressionType(ExpressionType.SQL)
            .inputSerialization(inputSerialization)
            .outputSerialization(outputSerialization)
            .build();
    }
}