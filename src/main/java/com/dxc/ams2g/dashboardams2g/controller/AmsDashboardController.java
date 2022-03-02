package com.dxc.ams2g.dashboardams2g.controller;

import com.dxc.ams2g.dashboardams2g.service.AmsDashboardService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;

@RestController
@RequestMapping
public class AmsDashboardController {
    @Autowired
    private AmsDashboardService service;

    @GetMapping(value = "switch/matching/csv")
    public ResponseEntity<String> findMatchingSwitch(
            @RequestParam(name = "dateFrom", required = false) Date dateFrom
    ) throws JsonProcessingException {
        return ResponseEntity.ok(service.findMatchingSwitchCsv(dateFrom));
    }

    @GetMapping(value = "volture/matching/csv")
    public ResponseEntity<String> findMatchingVolture(
            @RequestParam(name = "dateFrom", required = false) Date dateFrom
    ) throws JsonProcessingException {
        return ResponseEntity.ok(service.findMatchingVoltureCsv(dateFrom));
    }

    @GetMapping(value = "switch/matching/maxUploadDate/{maxUploadDate}")
    public ResponseEntity<List<Document>> findMatchingSwitchMaxUploadDate(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingSwitchMaxUploadDate(maxUploadDate == null ? new Date(): maxUploadDate));
    }

    @GetMapping(value = "switch/matching/maxUploadDate/{maxUploadDate}/count")
    public ResponseEntity<Integer> findMatchingSwitchMaxUploadDateCount(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ){
        return ResponseEntity.ok(service.findMatchingSwitchMaxUploadDate(maxUploadDate == null ? new Date(): maxUploadDate).size());
    }

    @GetMapping(value = "volture/matching/maxUploadDate/{maxUploadDate}")
    public ResponseEntity<List<Document>> findMatchingVoltureMaxUploadDate(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingVoltureMaxUploadDate(maxUploadDate == null ? new Date(): maxUploadDate));
    }

    @GetMapping(value = "volture/matching/maxUploadDate/{maxUploadDate}/count")
    public ResponseEntity<Integer> findMatchingVoltureMaxUploadDateCount(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ){
        return ResponseEntity.ok(service.findMatchingVoltureMaxUploadDate(maxUploadDate == null ? new Date(): maxUploadDate).size());
    }
}
