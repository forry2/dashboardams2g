package com.dxc.ams2g.dashboardams2g.controller;

import com.dxc.ams2g.dashboardams2g.service.AmsDashboardService;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.models.Response;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@RestController
@CrossOrigin
@RequestMapping
public class AmsDashboardController {
    @Autowired
    private AmsDashboardService service;

    @GetMapping(value = "switch/matching/csv")
    public ResponseEntity<String> findMatchingSwitchCsv() throws JsonProcessingException {
        return ResponseEntity.ok(service.findMatchingSwitchCsv());
    }

    @GetMapping(value = "switch/sides/json")
    public ResponseEntity<List<Document>> findSidesSwitchesDocList(){
        return ResponseEntity.ok(service.findSidesSwitchesDocList());
    }

    @GetMapping(value = "switch/sides/csv")
    public ResponseEntity<String> findSidesSwitchCsv() throws JsonProcessingException {
        return ResponseEntity.ok(service.findSidesSwitchCsvString());
    }

    @GetMapping(value = "switch/sides/csv/file")
    public ResponseEntity<Resource> findSidesSwitchCsvFile() {
        String currentDatetime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=sidesSwitchReport_" + currentDatetime + ".csv")
                .body(new InputStreamResource(service.exportSidesSwitchCsvFile()));
    }

    @GetMapping(value = "volture/sides/csv/file")
    public ResponseEntity<Resource> findSidesVoltureCsvFile() {
        String currentDatetime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=sidesVoltureReport_" + currentDatetime + "_.csv")
                .body(new InputStreamResource(service.exportSidesVoltureCsvFile()));
    }

    @GetMapping(value = "volture/matching/csv")
    public ResponseEntity<String> findMatchingVoltureCsv() throws JsonProcessingException {
        return ResponseEntity.ok(service.findMatchingVoltureCsv());
    }

    @GetMapping(value = "volture/sides/json")
    public ResponseEntity<List<Document>> findSidesVoltureDocList(){
        return ResponseEntity.ok(service.findSidesVoltureDocList());
    }

    @GetMapping(value = "volture/sides/csv")
    public ResponseEntity<String> findSidesVoltureCsv() throws JsonProcessingException {
        return ResponseEntity.ok(service.findSidesVoltureCsvString());
    }

    @GetMapping(value = "switch/matching/maxUploadDate/{maxUploadDate}")
    public ResponseEntity<List<Document>> findMatchingSwitchMaxUploadDate(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingSwitchMaxUploadDate(maxUploadDate == null ? new Date() : maxUploadDate));
    }

    @GetMapping(value = "switch/matching/maxUploadDate/{maxUploadDate}/timeWindowDays/{timeWindowDays}")
    public ResponseEntity<List<Document>> findSwitchCountersMaxUploadDateTimePeriod(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate,
            @PathVariable(value = "timeWindowDays") int timeWindowDays
    ) {
        Date pMaxUploadDate = (maxUploadDate == null ? new Date() : maxUploadDate);
        return ResponseEntity.ok(service.findSwitchCountersMaxUploadDateTimePeriod(pMaxUploadDate, timeWindowDays));
    }

    @GetMapping(value = "switch/matching/maxUploadDate/{maxUploadDate}/count")
    public ResponseEntity<Integer> findMatchingSwitchMaxUploadDateCount(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingSwitchMaxUploadDate(maxUploadDate == null ? new Date() : maxUploadDate).size());
    }

    @GetMapping(value = "volture/matching/maxUploadDate/{maxUploadDate}")
    public ResponseEntity<List<Document>> findMatchingVoltureMaxUploadDate(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingVoltureMaxUploadDate(maxUploadDate == null ? new Date() : maxUploadDate));
    }

    @GetMapping(value = "volture/matching/maxUploadDate/{maxUploadDate}/timeWindowDays/{timeWindowDays}")
    public ResponseEntity<List<Document>> findVoltureCountersMaxUploadDateTimePeriod(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate,
            @PathVariable(value = "timeWindowDays") int timeWindowDays
    ) {
        Date pMaxUploadDate = (maxUploadDate == null ? new Date() : maxUploadDate);
        return ResponseEntity.ok(service.findVoltureCountersMaxUploadDateTimePeriod(pMaxUploadDate, timeWindowDays));
    }

    @GetMapping(value = "volture/matching/maxUploadDate/{maxUploadDate}/count")
    public ResponseEntity<Integer> findMatchingVoltureMaxUploadDateCount(
            @PathVariable(value = "maxUploadDate") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") Date maxUploadDate
    ) {
        return ResponseEntity.ok(service.findMatchingVoltureMaxUploadDate(maxUploadDate == null ? new Date() : maxUploadDate).size());
    }

    @GetMapping(value = "lettureTecnicheSides")
    public ResponseEntity<List<Document>> findLettureTecnicheSides(){
        return ResponseEntity.ok(service.findLettureTecnicheSides());
    }

    @GetMapping(value = "periodicheSidEB")
    public ResponseEntity<List<Document>> findPeriodicheSidEB(){
        return ResponseEntity.ok(service.findPeriodicheSidEB());
    }

    @GetMapping(value = "puntiOdlNonChiuso")
    public ResponseEntity<List<Document>> findPuntiOdlNonChiuso(){
        return ResponseEntity.ok(service.findPuntiOdlNonChiuso());
    }

    @GetMapping(value = "sidesInviiNull")
    public ResponseEntity<List<Document>> findSidesInviiNull(){
        return ResponseEntity.ok(service.findSidesInviiNull());
    }

    @GetMapping(value = "validazioneTotaleFonteTb")
    public ResponseEntity<List<Document>> findValidazioneTotaleFonteTb(){
        return ResponseEntity.ok(service.findValidazioneTotaleFonteTb());
    }
}
