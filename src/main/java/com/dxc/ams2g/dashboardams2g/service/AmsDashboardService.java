package com.dxc.ams2g.dashboardams2g.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@Service
public class AmsDashboardService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private MongoTemplate mongoTemplate;

    public String findMatchingSwitchCsv() throws JsonProcessingException {

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

    public String findMatchingVoltureCsv() throws JsonProcessingException {
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

    public String findSidesVoltureCsvString() throws JsonProcessingException {
        List<Document> retList = findSidesCsv("dashboardAms2gVolture");
        log.debug("Found {} results", retList.size());
        JsonNode jsonTree = new ObjectMapper().readTree(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(retList));
        Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader().withLineSeparator("|");
        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(jsonTree);
    }

    public String findSidesSwitchCsvString() throws JsonProcessingException {
        List<Document> retList = findSidesCsv("dashboardAms2gSwitch");
        log.debug("Found {} results", retList.size());
        JsonNode jsonTree = new ObjectMapper().readTree(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(retList));
        Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader().withLineSeparator("|");
        CsvMapper csvMapper = new CsvMapper();
        return csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValueAsString(jsonTree);
    }


    public List<Document> findSidesSwitchesDocList() {
        return findSidesCsv("dashboardAms2gSwitch");
    }

    public ByteArrayInputStream exportSidesSwitchCsvFile() {
        return exportSidesCsvFile("dashboardAms2gSwitch");
    }

    public ByteArrayInputStream exportSidesVoltureCsvFile() {
        return exportSidesCsvFile("dashboardAms2gVolture");
    }

    public ByteArrayInputStream exportSidesCsvFile(String collectionName) {
        List<Document> retList = findSidesCsv(collectionName);
        if (retList.size() == 0) return null;
        String headers = String.join(";", retList.get(0).keySet()) + ";filename;validfrom;";
        final CSVFormat format = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(out), format)) {
            csvPrinter.printRecord(headers);
            for (Document doc : retList) {
                StringBuilder dataRow = new StringBuilder();
                for (String fieldName : headers.split(";")) {
                    dataRow.append(doc.get(fieldName) == null ? "" : doc.get(fieldName).toString());
                    dataRow.append(";");
                }
                csvPrinter.printRecord(dataRow.toString());
            }
            csvPrinter.flush();
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Document> findSidesCsv(String collectionName) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(new Criteria().and("lettura").ne(null)));
        AggregationOperation addExabeatMatchFieldOperation = context -> {
            Document gtDocument = new Document("$gt", Arrays.asList("$dettaglioPubblicazione", null));
            Document condDoc = new Document("$cond", Arrays.asList(gtDocument, true, false));
            Document exabeatMatchDoc = new Document("exabeatMatch", condDoc);
            return new Document("$addFields", exabeatMatchDoc);
        };
        aggrList.add(addExabeatMatchFieldOperation);

        String opDateName = (collectionName.equals("dashboardAms2gSwitch") ? "DSWITC" : "DVOLTU");

        aggrList.add(project("IDN_UTEN_ERN", opDateName).and("lettura.DAT_LETTURA_SID").as("DAT_LETTURA_SID").and("lettura.COD_PRESTAZIONE").as("COD_PRESTAZIONE").and("lettura.DAT_LETTURA_ERN").as("DAT_LETTURA_ERN").and("lettura.COD_PRATICA").as("COD_PRATICA").and("lettura.COD_STATO_PRATICA").as("COD_STATO_PRATICA").and("lettura.DES_TRATT_ORARIO").as("DES_TRATT_ORARIO").and("lettura.DES_TRATT_LETTURA").as("DES_TRATT_LETTURA").and("lettura.FLG_RETT").as("FLG_RETT").and("lettura.FLG_LDV").as("FLG_LDV").and("lettura.FLG_LDF").as("FLG_LDF").and("lettura.DAT_CREAZIONE_REC").as("DAT_CREAZIONE_REC").and("lettura.DAT_ULT_AGG_REC").as("DAT_ULT_AGG_REC").and("lettura.COD_FLG_LTU_EFV").as("COD_FLG_LTU_EFV").and("lettura.COD_FLG_REGIME").as("COD_FLG_REGIME").and("lettura.COD_FLG_TIP_ODL").as("COD_FLG_TIP_ODL").and("lettura.LTU_ETR").as("LTU_ETR").and("lettura.SOURCE_CODE").as("SOURCE_CODE").and("lettura.SOURCE_DETAIL").as("SOURCE_DETAIL").and("lettura.SOURCE_TYPE").as("SOURCE_TYPE").and("lettura.TIPO_LAVORO").as("TIPO_LAVORO").and("lettura.TIP_LTU").as("TIP_LTU").and("lettura.WO_ACTIVITY").as("WO_ACTIVITY").and("exabeatMatch").as("exabeatMatch").and("$dettaglioPubblicazione.filename").as("filename").and("$dettaglioPubblicazione.validfrom").as("validfrom").andExclude("_id"));
        aggrList.add(sort(Sort.Direction.DESC, opDateName).and(Sort.Direction.ASC, "IDN_UTEN_ERN"));
        return mongoTemplate.aggregate(newAggregation(aggrList), collectionName, Document.class).getMappedResults();
    }

    public List<Document> findMatchingSwitchMaxUploadDate(Date maxUploadDate) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(where("dataUploadDateTime").lte(maxUploadDate)));
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
        aggrList.add(match(where("sourcesCount").gte(2)));
        return mongoTemplate.aggregate(newAggregation(aggrList).withOptions(AggregationOptions.builder().allowDiskUse(true).build()), "dashboardAms2gSwitchRaw", Document.class).getMappedResults();
    }

    public List<Document> findSwitchCountersMaxUploadDateTimePeriod(Date pMaxUploadDate, int timeWindowDays) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(where("dataSource").is("sides")));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document uploadDatesVal = new Document("$push", "$dataUploadDateTime");
            Document idVal = new Document("DSWITC", "$DSWITC").append("IDN_UTEN_ERN", "$IDN_UTEN_ERN");
            Document groupVal = new Document("_id", idVal).append("uploadDates", uploadDatesVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);

        aggrList.add(project("uploadDates").and("$_id.DSWITC").as("DSWITC").and("$_id.IDN_UTEN_ERN").as("IDN_UTEN_ERN").andExclude("_id"));

        FacetOperation sidesOnlyFacetOperation = new FacetOperation();
        for (int counter = 0; counter < timeWindowDays; counter++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(counter);
            MatchOperation matchOperation = match(where("uploadDates").lte(runningDate));
            CountOperation countOperation = count().as("sidesOnlyCount");
            sidesOnlyFacetOperation = sidesOnlyFacetOperation.and(matchOperation, countOperation).as(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")));
        }
        aggrList.add(sidesOnlyFacetOperation);
        Document sidesOnlyCountList = mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAms2gSwitchRaw", Document.class).getUniqueMappedResult();

        HashMap<String, Document> retMap = new HashMap<String, Document>();
        assert sidesOnlyCountList != null;
        sidesOnlyCountList.keySet().forEach(s -> {
            List<Document> keyDoc = (List) sidesOnlyCountList.get(s);
            retMap.put(s, new Document("sidesOnlyCount", (keyDoc.size() > 0 ? keyDoc.get(0).get("sidesOnlyCount") : 0)));
        });


        FacetOperation sidesExabeatMatchFacetOperation = new FacetOperation();
        for (int counter = 0; counter < timeWindowDays; counter++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(counter);
            MatchOperation matchOperation = match(where("dataUploadDateTime").lte(runningDate));
            AggregationOperation customGroupOperation = aoc -> {
                Document dataSources = new Document("$push", "$dataSource");
                Document id = new Document("DSWITC", "$DSWITC").append("IDN_UTEN_ERN", "$IDN_UTEN_ERN");
                Document group = new Document("_id", id).append("dataSources", dataSources);
                return new Document("$group", group);
            };
            MatchOperation secondMatchOperation = match(where("dataSources").all("sides", "exabeat"));
            CountOperation countOperation = count().as("matchesCount");
            sidesExabeatMatchFacetOperation = sidesExabeatMatchFacetOperation.and(matchOperation, customGroupOperation, secondMatchOperation, countOperation).as(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")));
        }
        Document sidesExabeatMatchList = mongoTemplate.aggregate(newAggregation(sidesExabeatMatchFacetOperation), "dashboardAms2gSwitchRaw", Document.class).getUniqueMappedResult();
        sidesExabeatMatchList.keySet().forEach(s -> {
            Document retMapDoc = retMap.get(s);
            if (retMapDoc == null) retMapDoc = new Document("sidesOnlyCount", 0);
            List<Document> keyDoc = (List) sidesExabeatMatchList.get(s);
            retMapDoc.append("matchesCount", (keyDoc.size() > 0 ? keyDoc.get(0).get("matchesCount") : 0));
            Integer matchesCount = retMapDoc.getInteger("matchesCount");
            Integer sidesOnlyCount = retMapDoc.getInteger("sidesOnlyCount");
            retMapDoc.append("deltaCount", sidesOnlyCount - matchesCount);
            retMap.put(s, retMapDoc);

        });
        List<Document> retList = new ArrayList<>();
        for (int counter = 0; counter < timeWindowDays; counter++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(counter);
            retList.add(new Document(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")), retMap.get(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")))));
        }


//        Integer maxSides = null;
//        Integer minSides = null;
//        Integer maxMatches = null;
//        Integer minMatches = null;
//        for (Document document : retMap.values()){
//            if (maxSides == null || minSides == null || maxMatches == null || minMatches == null)
//            {
//                maxSides = document.getInteger("sidesOnlyCount");
//                minSides = document.getInteger("sidesOnlyCount");
//                maxMatches = document.getInteger("matchesCount");
//                minMatches = document.getInteger("matchesCount");
//            }
//            else{
//                maxSides = (maxSides < document.getInteger("sidesOnlyCount") ? document.getInteger("sidesOnlyCount") : maxSides);
//                minSides = (minSides > document.getInteger("sidesOnlyCount") ? document.getInteger("sidesOnlyCount") : minSides);
//                maxMatches = (maxMatches < document.getInteger("matchesCount") ? document.getInteger("matchesCount") : maxMatches);
//                minMatches = (minMatches > document.getInteger("matchesCount") ? document.getInteger("matchesCount") : minMatches);
//            }
//        }

        Collections.reverse(retList);
//        retList.add(new Document("minimums", new Document("sidesOnlyCount", minSides).append("matchesCount", minMatches)));
//        retList.add(new Document("maximums", new Document("sidesOnlyCount", maxSides).append("matchesCount", maxMatches)));
        return retList;
    }


    public List<Document> findMatchingVoltureMaxUploadDate(Date maxUploadDate) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(where("dataUploadDateTime").lte(maxUploadDate)));
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
        aggrList.add(match(where("sourcesCount").gte(2)));
        return mongoTemplate.aggregate(newAggregation(aggrList).withOptions(AggregationOptions.builder().allowDiskUse(true).build()), "dashboardAms2gVoltureRaw", Document.class).getMappedResults();
    }

    public List<Document> findVoltureCountersMaxUploadDateTimePeriod(Date pMaxUploadDate, int timeWindowDays) {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();
        aggrList.add(match(where("dataSource").is("sides")));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document uploadDatesVal = new Document("$push", "$dataUploadDateTime");
            Document idVal = new Document("DVOLTU", "$DVOLTU").append("IDN_UTEN_ERN", "$IDN_UTEN_ERN");
            Document groupVal = new Document("_id", idVal).append("uploadDates", uploadDatesVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);

        aggrList.add(project("uploadDates").and("$_id.DVOLTU").as("DVOLTU").and("$_id.IDN_UTEN_ERN").as("IDN_UTEN_ERN").andExclude("_id"));

        FacetOperation sidesOnlyFacetOperation = new FacetOperation();
        for (int i = 0; i < timeWindowDays; i++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(i);
            MatchOperation matchOperation = match(where("uploadDates").lte(runningDate));
            CountOperation countOperation = count().as("sidesOnlyCount");
            sidesOnlyFacetOperation = sidesOnlyFacetOperation.and(matchOperation, countOperation).as(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")));
        }
        aggrList.add(sidesOnlyFacetOperation);
        Document sidesOnlyCountList = mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAms2gVoltureRaw", Document.class).getUniqueMappedResult();

        HashMap<String, Document> retMap = new HashMap<String, Document>();
        sidesOnlyCountList.keySet().forEach(s -> {
            List<Document> keyDoc = (List) sidesOnlyCountList.get(s);
            retMap.put(s, new Document("sidesOnlyCount", (keyDoc.size() > 0 ? keyDoc.get(0).get("sidesOnlyCount") : 0)));
        });

        FacetOperation sidesExabeatMatchFacetOperation = new FacetOperation();
        for (int i = 0; i < timeWindowDays; i++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(i);
            MatchOperation matchOperation = match(where("dataUploadDateTime").lte(runningDate));
            AggregationOperation customGroupOperation = aoc -> {
                Document dataSources = new Document("$push", "$dataSource");
                Document id = new Document("DVOLTU", "$DVOLTU").append("IDN_UTEN_ERN", "$IDN_UTEN_ERN");
                Document group = new Document("_id", id).append("dataSources", dataSources);
                return new Document("$group", group);
            };
            MatchOperation secondMatchOperation = match(where("dataSources").all("sides", "exabeat"));
            CountOperation countOperation = count().as("matchesCount");
            sidesExabeatMatchFacetOperation = sidesExabeatMatchFacetOperation.and(matchOperation, customGroupOperation, secondMatchOperation, countOperation).as(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")));
        }
        Document sidesExabeatMatchList = mongoTemplate.aggregate(newAggregation(sidesExabeatMatchFacetOperation), "dashboardAms2gVoltureRaw", Document.class).getUniqueMappedResult();
        sidesExabeatMatchList.keySet().forEach(s -> {
            Document retMapDoc = retMap.get(s);
            if (retMapDoc == null) retMapDoc = new Document("sidesOnlyCount", 0);
            List<Document> keyDoc = (List) sidesExabeatMatchList.get(s);
            retMapDoc.append("matchesCount", (keyDoc.size() > 0 ? keyDoc.get(0).get("matchesCount") : 0));
            Integer matchesCount = retMapDoc.getInteger("matchesCount");
            Integer sidesOnlyCount = retMapDoc.getInteger("sidesOnlyCount");
            retMapDoc.append("deltaCount", sidesOnlyCount - matchesCount);
            retMap.put(s, retMapDoc);

        });

        List<Document> retList = new ArrayList<>();
        for (int i = 0; i < timeWindowDays; i++) {
            LocalDateTime runningDate = LocalDateTime.of(pMaxUploadDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalTime.MIDNIGHT).minusDays(i);
            retList.add(new Document(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")), retMap.get(runningDate.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")))));
        }

//
//        Integer maxSides = null;
//        Integer minSides = null;
//        Integer maxMatches = null;
//        Integer minMatches = null;
//        for (Document document : retMap.values()){
//            if (maxSides == null || minSides == null || maxMatches == null || minMatches == null)
//            {
//                maxSides = document.getInteger("sidesOnlyCount");
//                minSides = document.getInteger("sidesOnlyCount");
//                maxMatches = document.getInteger("matchesCount");
//                minMatches = document.getInteger("matchesCount");
//            }
//            else{
//                maxSides = (maxSides < document.getInteger("sidesOnlyCount") ? document.getInteger("sidesOnlyCount") : maxSides);
//                minSides = (minSides > document.getInteger("sidesOnlyCount") ? document.getInteger("sidesOnlyCount") : minSides);
//                maxMatches = (maxMatches < document.getInteger("matchesCount") ? document.getInteger("matchesCount") : maxMatches);
//                minMatches = (minMatches > document.getInteger("matchesCount") ? document.getInteger("matchesCount") : minMatches);
//            }
//        }
        Collections.reverse(retList);
//        retList.add(new Document("minimums", new Document("sidesOnlyCount", minSides).append("matchesCount", minMatches)));
//        retList.add(new Document("maximums", new Document("sidesOnlyCount", maxSides).append("matchesCount", maxMatches)));
        return retList;
    }

    public List<Document> dashboardAmsMonitorPeriodicheSidEB() {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();

        aggrList.add(sort(Sort.Direction.DESC, "dataUploadDateTime"));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document firstDocVal = new Document("$first", "$$ROOT");
            Document idVal = new Document("IDN_UTEN_ERN", "$IDN_UTEN_ERN").append("DAT_LETTURA", "$DAT_LETTURA");
            Document groupVal = new Document("_id", idVal).append("firstDoc", firstDocVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);
        aggrList.add(replaceRoot("firstDoc"));
        aggrList.add(project().andExclude("$_id", "dataUploadDateTime"));
        aggrList.add(sort(Sort.Direction.DESC, "DAT_LETTURA").and(Sort.Direction.ASC,"IDN_UTEN_ERN"));

        return mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAmsMonitorPeriodicheSidEB", Document.class).getMappedResults();
    }

    public List<Document> dashboardAmsMonitorPuntiOdlNonChiuso() {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();

        aggrList.add(sort(Sort.Direction.DESC, "dataUploadDateTime"));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document firstDocVal = new Document("$first", "$$ROOT");
            Document idVal = new Document("IDN_UTEN_ERN", "$IDN_UTEN_ERN").append("DAT_LETTURA", "$DAT_LETTURA");
            Document groupVal = new Document("_id", idVal).append("firstDoc", firstDocVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);
        aggrList.add(replaceRoot("firstDoc"));
        aggrList.add(project().andExclude("$_id", "dataUploadDateTime"));
        aggrList.add(sort(Sort.Direction.DESC, "DAT_LETTURA").and(Sort.Direction.ASC, "IDN_UTEN_ERN"));

        return mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAmsMonitorPuntiOdlNonChiuso", Document.class).getMappedResults();
    }

    public List<Document> dashboardAmsMonitorSidesInviiNull() {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();

        aggrList.add(sort(Sort.Direction.DESC, "dataUploadDateTime"));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document firstDocVal = new Document("$first", "$$ROOT");
            Document idVal = new Document("IDN_UTEN_ERN", "$IDN_UTEN_ERN").append("DAT_LETTURA_SID", "$DAT_LETTURA_SID");
            Document groupVal = new Document("_id", idVal).append("firstDoc", firstDocVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);
        aggrList.add(replaceRoot("firstDoc"));
        aggrList.add(project().andExclude("$_id", "dataUploadDateTime"));
        aggrList.add(sort(Sort.Direction.DESC, "DAT_LETTURA_SID").and(Sort.Direction.ASC, "IDN_UTEN_ERN"));

        return mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAmsMonitorSidesInviiNull", Document.class).getMappedResults();
    }

    public List<Document> dashboardAmsMonitorValidazioneTotaleFonteTb() {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();

        aggrList.add(sort(Sort.Direction.DESC, "dataUploadDateTime"));

        AggregationOperation customGroupAggrOperation = aoc -> {
            Document firstDocVal = new Document("$first", "$$ROOT");
            Document idVal = new Document("COD_PRIORITA", "$COD_PRIORITA").append("DES_CAUSA_SCARTO", "$DES_CAUSA_SCARTO");
            Document groupVal = new Document("_id", idVal).append("firstDoc", firstDocVal);
            return new Document("$group", groupVal);
        };
        aggrList.add(customGroupAggrOperation);
        aggrList.add(replaceRoot("firstDoc"));
        aggrList.add(project().andExclude("$_id", "dataUploadDateTime"));
        aggrList.add(sort(Sort.Direction.ASC, "DES_CAUSA_SCARTO", "COD_PRIORITA"));

        return mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAmsMonitorValidazioneTotaleFonteTb", Document.class).getMappedResults();
    }

    public List<Document> findSidesVoltureDocList() {
        return findSidesCsv("dashboardAms2gVolture");

    }

    public List<Document> dashboardAmsLettureTecnicheSides() {
        ArrayList<AggregationOperation> aggrList = new ArrayList<>();

        aggrList.add(sort(Sort.Direction.DESC, "dataUploadDateTime"));

        aggrList.add(group("$IDN_UTEN_ERN", "$DAT_LETTURA").first("$$ROOT").as("lettura"));

        aggrList.add(
                project()
                        .andExclude("_id")
                        .and("$_id.IDN_UTEN_ERN").as("IDN_UTEN_ERN")
                        .and("$_id.DAT_LETTURA").as("DAT_LETTURA")
                        .and("$lettura.dataUploadDateTime").as("dataUploadDateTime")
                        .and("$lettura.COD_TIPO_LETTURA").as("COD_TIPO_LETTURA")
                        .and("$lettura.DAT_SCARICO_LTU").as("DAT_SCARICO_LTU")
                        .and("$lettura.NUM_MTC_APPAR").as("NUM_MTC_APPAR")
                        .and("$lettura.COD_TIPO_FONTE").as("COD_TIPO_FONTE")
                        .and("$lettura.COD_TIPO_FONTE_ERN").as("COD_TIPO_FONTE_ERN")
                        .and("$lettura.NUM_ODL").as("NUM_ODL")
                        .and("$lettura.COD_TIPOLOG_PRATIC").as("COD_TIPOLOG_PRATIC")
                        .and("$lettura.DES_TIPOLOG_PRATIC").as("DES_TIPOLOG_PRATIC")
        );

        aggrList.add(sort(Sort.Direction.DESC, "DAT_LETTURA").and(Sort.Direction.ASC, "IDN_UTEN_ERN"));

        return mongoTemplate.aggregate(newAggregation(aggrList), "dashboardAmsLettureTecnicheSides", Document.class).getMappedResults();
    }
}
