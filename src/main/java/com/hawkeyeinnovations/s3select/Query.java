package com.hawkeyeinnovations.s3select;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.experimental.UtilityClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@UtilityClass
public class Query {

    private static final ListeningExecutorService processingThreadPool = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("Processing-Thread-%d")
                .build()
        )
    );

    private static final ExecutorService completionThreadPool = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("Completion-Thread-%d")
            .build()
    );

    public static void query(S3AsyncClient s3Client, String bucket, String key,
                             String outputPath, String query) throws Exception {
        CompletableFuture<ListObjectsV2Response> listObjectsResponseFuture = s3Client.listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(key)
                .build()
        );
        ListObjectsV2Response listObjectsResponse = listObjectsResponseFuture.get();
        processFiles(listObjectsResponse, bucket, query, outputPath, s3Client);
        while (listObjectsResponse.isTruncated()) {
            String continuationToken = listObjectsResponse.nextContinuationToken();
            listObjectsResponseFuture = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(continuationToken)
                    .build()
            );
            listObjectsResponse = listObjectsResponseFuture.get();
            processFiles(listObjectsResponse, bucket, query, outputPath, s3Client);
        }
    }

    private static void processFiles(ListObjectsV2Response response, String bucket, String query, String outputPath,
                                     S3AsyncClient s3Client) {
        List<S3Object> s3Objects = response.contents();
        processingThreadPool.submit(() -> getFiles(s3Objects, bucket, query, outputPath, s3Client))
            .addListener(() -> System.out.println("Completed 1000 files"), completionThreadPool);
    }

    public static void getFiles(List<S3Object> s3Objects, String bucket, String query, String outputPath,
                                S3AsyncClient s3Client) {
        s3Objects.forEach(s3Object -> {
            SelectObjectContentRequest request = generateRequestCSV(bucket, s3Object.key(), query);
            try {
                s3Client.selectObjectContent(request, SelectObjectContentResponseHandler.builder()
                    .subscriber(SelectObjectContentResponseHandler.Visitor.builder()
                        .onRecords(event -> {
                            if (!Files.exists(Path.of(outputPath + File.separator + s3Object.key()))) {
                                System.out.println("Writing file " + s3Object.key() + "...");
                                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(s3Object.key())
                                    .build();
                                try {
                                    s3Client.getObject(getObjectRequest, Path.of(outputPath + File.separator + s3Object.key())).get();
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                }
                            }
                        })
                        .build())
                    .build())
                    .get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
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
}