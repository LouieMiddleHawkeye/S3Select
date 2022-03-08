package com.hawkeyeinnovations.s3select;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.utils.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.hawkeyeinnovations.s3select.Query.query;

@Command(name = "ParameterLoader", description = "Parameters for what files you want to pull back")
public class QueryLoader implements Runnable {

    @Option(names = {"-r", "--region"}, description = "Region the bucket is in", required = true)
    private String region;

    @Option(names = {"-bn", "--bucketName"}, description = "Name of bucket with files", required = true)
    private String bucketName;

    @Option(names = {"-k", "--key"}, description = "Key of file(s) to look through")
    private String key;

    @Option(names = {"-op", "--outputPath"}, description = "Output path for resulting files")
    private String outputPath;

    @Option(names = {"-q", "--query"}, description = "Query to perform", required = true)
    private String query;

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        System.out.println("Starting query");

        S3AsyncClient s3Client = S3AsyncClient.builder()
            .region(Region.of(region))
            .build();

        if (StringUtils.isBlank(outputPath)) {
            new File(QueryLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath()
                + File.separator + "results");
        } else if (!Files.exists(Paths.get(outputPath))) {
            new File(outputPath).mkdirs();
        }
        try {
            query(s3Client, bucketName, key, outputPath, query);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Took " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + "s");
    }
}
