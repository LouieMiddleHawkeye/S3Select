package com.hawkeyeinnovations.s3select.service;

import com.hawkeyeinnovations.dataconnections.s3.S3Utils;
import com.hawkeyeinnovations.s3select.model.FileType;
import com.hawkeyeinnovations.s3select.model.api.QueryRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class QueryService {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @PreDestroy
    public void close() {
        executorService.shutdownNow();
    }

    public void query(QueryRequest request, FileType fileType) {
        log.info("Starting query");

        List<String> prefixes = request.getPrefixes();
        String outputPath = request.getOutputPath();
        String query = request.getQuery();
        String bucket = request.getBucketName();
        String regionString = request.getRegion();

        Region region = Region.of(regionString);

        if (!Files.exists(Paths.get(outputPath))) {
            if (!new File(outputPath).mkdirs()) {
                log.error("Failed to create output directory {}", outputPath);
                return;
            }
        }

        prefixes.forEach(prefix -> {
            S3AsyncClient s3Client = S3AsyncClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.builder()
                    .profileName("okta")
                    .build())
                .build();
            S3Utils s3Utils = new S3Utils(s3Client, bucket, prefix, region);

            String prefixPath = outputPath + File.separator + prefix;
            if (!Files.exists(Paths.get(prefixPath))) {
                if (!new File(prefixPath).mkdirs()) {
                    log.error("Failed to create prefix directory {}", prefixPath);
                    return;
                }
            }

            List<String> files = s3Utils.getFilesWithPrefix(prefix);

            files.forEach(file -> {
                Query runnableQuery = new Query(file, query, prefixPath, s3Client, fileType, bucket, prefix);
                CompletableFuture.runAsync(runnableQuery, executorService).whenComplete((r, e) -> log.info("Finished processing file {}", file));
            });
        });
    }
}