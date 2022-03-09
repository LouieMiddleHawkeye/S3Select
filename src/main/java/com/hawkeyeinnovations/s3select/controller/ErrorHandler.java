package com.hawkeyeinnovations.s3select.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.List;
import java.util.stream.Collectors;

/*
  Translates javax validation messages into HTTP 400 (Bad Request) response with body like this:
    {
      "errorCode": "VALIDATION_ERROR",
      "validationMessages": [
        {
          "parameter": "fieldOne",
          "reason": "must not be null"
        },
        {
          "parameter": "fieldTwo",
          "reason": "must not be empty"
        }
      ]
    }
 */
@ControllerAdvice
public class ErrorHandler {

    private static final String VALIDATION_ERROR = "VALIDATION_ERROR";

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<ValidationFailedResponse> handleMissingArguments(MethodArgumentNotValidException exception) {
        List<ValidationFailedResponse.ValidationMessage> validationErrors = exception.getBindingResult().getAllErrors()
            .stream()
            .map(e -> new ValidationFailedResponse.ValidationMessage(getParameterName(e), e.getDefaultMessage()))
            .collect(Collectors.toList());

        return badRequest(validationErrors);
    }

    private String getParameterName(final ObjectError error) {
        if (error instanceof FieldError) {
            FieldError fieldError = (FieldError) error;
            return fieldError.getField();
        }
        return error.getObjectName();
    }

    private ResponseEntity<ValidationFailedResponse> badRequest(List<ValidationFailedResponse.ValidationMessage> errors) {
        ValidationFailedResponse errorResponse = new ValidationFailedResponse(VALIDATION_ERROR, errors);
        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }
}
