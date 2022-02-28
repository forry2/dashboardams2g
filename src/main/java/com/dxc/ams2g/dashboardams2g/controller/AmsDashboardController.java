package com.dxc.ams2g.dashboardams2g.controller;

import com.dxc.ams2g.dashboardams2g.service.AmsDashboardService;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping
public class AmsDashboardController {
    @Autowired
    private AmsDashboardService service;

    @GetMapping(value = "matching")
    public ResponseEntity<Document> findMatching(
            @RequestParam(name = "dateFrom", required = false) Date dateFrom
    ) {
        return null;
    }
}
