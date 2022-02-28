package com.dxc.ams2g.dashboardams2g.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
public class AmsDashboardService {
    @Autowired
    private MongoTemplate mongoTemplate;

    public String findMatchingSwitchCsv(Date dateFrom) throws JsonProcessingException {

        ArrayList<AggregationOperation> aggrList =
                new ArrayList<>();
        Criteria criteria = new Criteria();
        criteria = criteria.and("lettura").ne(null).and("dettaglioPubblicazione").ne(null);
        aggrList.add(match(criteria));
        aggrList.add(project("DSWITC", "IDN_UTEN_ERN").andExclude("_id"));
        aggrList.add(sort(Sort.Direction.DESC, "DSWITC", "IDN_UTEN_ERN"));
        JsonNode jsonTree = new ObjectMapper().readTree(
                new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(mongoTemplate
                        .aggregate(newAggregation(aggrList), "dashboardAms2gSwitch", Document.class)
                        .getMappedResults())
        );
        Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        return
                csvMapper.writerFor(JsonNode.class)
                        .with(csvSchema)
                        .writeValueAsString(jsonTree);
    }
}
