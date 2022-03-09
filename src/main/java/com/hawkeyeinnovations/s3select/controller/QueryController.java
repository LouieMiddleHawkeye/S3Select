package com.hawkeyeinnovations.s3select.controller;

import com.hawkeyeinnovations.s3select.model.FileType;
import com.hawkeyeinnovations.s3select.model.api.QueryRequest;
import com.hawkeyeinnovations.s3select.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "api/v1/query")
public class QueryController {

    private final QueryService queryService;

    @Autowired
    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    @PostMapping
    @Operation(summary = "Get all files that match query")
    @Parameter(name = "fileType", description = "Whether you are querying JSONs or CSVs")
    public ResponseEntity<?> query(@RequestBody QueryRequest request,
                                   @RequestParam FileType fileType)  {
        queryService.query(request, fileType);
        return ResponseEntity.accepted().build();
    }
}
