package com.dxc.ams2g.dashboardams2g.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
public class AmsDashboardService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private MongoTemplate mongoTemplate;

    public String findMatchingSwitchCsv(Date dateFrom) throws JsonProcessingException {

        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        Criteria criteria = new Criteria();
        criteria = criteria.and("lettura").ne(null).and("dettaglioPubblicazione").ne(null);
        aggrList.add(match(criteria));
        aggrList.add(project("DSWITC", "IDN_UTEN_ERN").andExclude("_id"));
        aggrList.add(sort(Sort.Direction.DESC, "DSWITC", "IDN_UTEN_ERN"));
        List<Document> retList = mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAms2gSwitch", Document.class).getMappedResults();
        log.debug("Found {} results", retList.size());
        JsonNode jsonTree = new ObjectMapper().readTree(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(retList));
        Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(jsonTree);
    }

    public String findMatchingVoltureCsv(Date dateFrom) throws JsonProcessingException {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        Criteria criteria = new Criteria();
        criteria = criteria.and("lettura").ne(null).and("dettaglioPubblicazione").ne(null);
        aggrList.add(match(criteria));
        aggrList.add(project("DVOLTU", "IDN_UTEN_ERN").andExclude("_id"));
        aggrList.add(sort(Sort.Direction.DESC, "DVOLTU", "IDN_UTEN_ERN"));
        List<Document> retList = mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAms2gVolture", Document.class).getMappedResults();
        log.debug("Found {} results", retList.size());
        JsonNode jsonTree = new ObjectMapper().readTree(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(retList));
        Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(jsonTree);
    }

    public List<Document> findMatchingSwitchMaxUploadDate(Date maxUploadDate) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(Criteria.where("dataUploadDateTime").lte(maxUploadDate)));
        aggrList.add(group("IDN_UTEN_ERN", "DSWITC", "dataSource").count().as("count"));
        AggregationOperation customGroupAggrOperation = context -> {
            Document idDoc = new Document();
            idDoc.put("IDN_UTEN_ERN", "$_id.IDN_UTEN_ERN");
            idDoc.put("DSWITC", "$_id.DSWITC");
            Document sourcesDoc = new Document("$push", "$_id.dataSource");
            Document groupDoc = new Document();
            groupDoc.put("_id", idDoc);
            groupDoc.put("sources", sourcesDoc);
            return new Document("$group", groupDoc);
        };
        aggrList.add(customGroupAggrOperation);
        AggregationOperation customAddFieldsOperation = aoc -> {
            Document sizeSourcesDoc = new Document("$size", "$sources");
            Document sourceCountDoc = new Document("sourcesCount", sizeSourcesDoc);
            return new Document("$addFields", sourceCountDoc);
        };
        aggrList.add(customAddFieldsOperation);
        AggregationOperation customProjectOperation = aoc -> {
            Document projDoc = new Document();
            projDoc.put("_id", 0);
            projDoc.put("IDN_UTEN_ERN", "$_id.IDN_UTEN_ERN");
            projDoc.put("DSWITC", "$_id.DSWITC");
            projDoc.put("sourcesCount", 1);
            return new Document("$project", projDoc);
        };
        aggrList.add(customProjectOperation);
        aggrList.add(match(Criteria.where("sourcesCount").gte(2)));
        return mongoTemplate.aggregate(newAggregation(aggrList).withOptions(AggregationOptions.builder().allowDiskUse(true).build()), "dashboardAms2gSwitchRaw", Document.class).getMappedResults();
    }

    public List<Document> findMatchingVoltureMaxUploadDate(Date maxUploadDate) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(Criteria.where("dataUploadDateTime").lte(maxUploadDate)));
        aggrList.add(group("IDN_UTEN_ERN", "DVOLTU", "dataSource").count().as("count"));
        AggregationOperation customGroupAggrOperation = context -> {
            Document idDoc = new Document();
            idDoc.put("IDN_UTEN_ERN", "$_id.IDN_UTEN_ERN");
            idDoc.put("DVOLTU", "$_id.DVOLTU");
            Document sourcesDoc = new Document("$push", "$_id.dataSource");
            Document groupDoc = new Document();
            groupDoc.put("_id", idDoc);
            groupDoc.put("sources", sourcesDoc);
            return new Document("$group", groupDoc);
        };
        aggrList.add(customGroupAggrOperation);
        AggregationOperation customAddFieldsOperation = aoc -> {
            Document sizeSourcesDoc = new Document("$size", "$sources");
            Document sourceCountDoc = new Document("sourcesCount", sizeSourcesDoc);
            return new Document("$addFields", sourceCountDoc);
        };
        aggrList.add(customAddFieldsOperation);
        AggregationOperation customProjectOperation = aoc -> {
            Document projDoc = new Document();
            projDoc.put("_id", 0);
            projDoc.put("IDN_UTEN_ERN", "$_id.IDN_UTEN_ERN");
            projDoc.put("DVOLTU", "$_id.DVOLTU");
            projDoc.put("sourcesCount", 1);
            return new Document("$project", projDoc);
        };
        aggrList.add(customProjectOperation);
        aggrList.add(match(Criteria.where("sourcesCount").gte(2)));
        return mongoTemplate.aggregate(newAggregation(aggrList).withOptions(AggregationOptions.builder().allowDiskUse(true).build()), "dashboardAms2gVoltureRaw", Document.class).getMappedResults();
    }
}
