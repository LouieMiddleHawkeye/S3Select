package com.hawkeyeinnovations.s3select.model.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@Value
@Builder
@JsonDeserialize(builder = QueryRequest.QueryRequestBuilder.class)
public class QueryRequest {

    @Schema(
        description = "Region bucket is in",
        example = "us-east-1",
        required = true
    )
    @NotEmpty
    private String region;

    @Schema(
        description = "The bucket",
        example = "bdp-feed-data-dev",
        required = true
    )
    @NotEmpty
    private String bucketName;

    @Schema(
        description = "The prefix",
        example = "[\"messages/2022/1_Major League Baseball/2392_Minute Maid Park/2022-03-07/tiluvyzr/segment.events/\", \"messages/2022/1_Major League Baseball/2392_Minute Maid Park/2022-03-07/tiluvyzr/segment.summary/\"]",
        required = false
    )
    private List<String> prefixes;

    @Schema(
        description = "Where you want your resulting files to be output",
        example = "C:/Users/Hawk-Eye/Documents/GitHub/S3Select/results",
        required = true
    )
    @NotEmpty
    private String outputPath;

    @Schema(
        description = "The SQL query",
        example = "SELECT * FROM S3Object s WHERE s.revision > 0",
        required = true
    )
    @NotEmpty
    private String query;

    @JsonPOJOBuilder(withPrefix = "")
    public static class QueryRequestBuilder {}
}
