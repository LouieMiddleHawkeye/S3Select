package com.hawkeyeinnovations.s3select.service;

import com.hawkeyeinnovations.s3select.model.FileType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Query implements Runnable {

    private final String file;
    private final String query;
    private final String path;
    private final S3AsyncClient s3Client;
    private final FileType fileType;
    private final String bucket;
    private final String prefix;

    public Query(String file, String query, String path, S3AsyncClient s3Client,
                 FileType fileType, String bucket, String prefix) {
        this.file = file;
        this.query = query;
        this.path = path;
        this.s3Client = s3Client;
        this.fileType = fileType;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        log.info("Running query '{}' on file '{}'", query, file);
        SelectObjectContentRequest request = generateRequest();
        try {
            s3Client.selectObjectContent(request, SelectObjectContentResponseHandler.builder()
                    .subscriber(SelectObjectContentResponseHandler.Visitor.builder()
                        .onRecords(event -> {
                            String outputLocation = path + File.separator + file.replace(prefix, "");
                            if (!Files.exists(Path.of(outputLocation))) {
                                log.info("Writing file at location " + outputLocation);
                                try (OutputStream outputStream = new FileOutputStream(outputLocation)) {
                                    outputStream.write(event.payload().asByteArray());
                                } catch (IOException e) {
                                    log.error("Failed to write file", e);
                                }
                            } else {
                                log.warn("Not writing to file as already exists at location {}", outputLocation);
                            }
                        })
                        .build())
                    .onResponse(response -> log.info("Query response code {}", response.sdkHttpResponse().statusCode()))
                    .build())
                .get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to run query '{}' on file '{}'", query, file, e);
        }
    }

    private SelectObjectContentRequest generateRequest() {
        Pair<InputSerialization, OutputSerialization> pair = FileType.JSON.equals(fileType) ?
            generateSerializersJSON() : generateSerializersCSV();

        return SelectObjectContentRequest.builder()
            .bucket(bucket)
            .key(file)
            .expression(query)
            .expressionType(ExpressionType.SQL)
            .inputSerialization(pair.getLeft())
            .outputSerialization(pair.getRight())
            .build();
    }

    private Pair<InputSerialization, OutputSerialization> generateSerializersCSV() {
        InputSerialization inputSerialization = InputSerialization.builder()
            .csv(CSVInput.builder().build())
            .compressionType(CompressionType.NONE)
            .build();

        OutputSerialization outputSerialization = OutputSerialization.builder()
            .csv(CSVOutput.builder().build())
            .build();

        return Pair.of(inputSerialization, outputSerialization);
    }

    private Pair<InputSerialization, OutputSerialization> generateSerializersJSON() {
        InputSerialization inputSerialization = InputSerialization.builder()
            .json(JSONInput.builder()
                .type(JSONType.DOCUMENT)
                .build())
            .compressionType(CompressionType.NONE)
            .build();

        OutputSerialization outputSerialization = OutputSerialization.builder()
            .json(JSONOutput.builder().build())
            .build();

        return Pair.of(inputSerialization, outputSerialization);
    }
}
