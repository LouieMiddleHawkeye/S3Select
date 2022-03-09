package com.hawkeyeinnovations.s3select.controller;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Value;

import java.util.List;

@Value
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ValidationFailedResponse {

    @Schema(title = "Error code", example = "VALIDATION_ERROR")
    private final String errorCode;

    @Schema(title = "Error message(s)")
    private final List<ValidationMessage> validationMessages;

    @Value
    public static class ValidationMessage {
        private final String parameter;
        private final String reason;
    }
}
