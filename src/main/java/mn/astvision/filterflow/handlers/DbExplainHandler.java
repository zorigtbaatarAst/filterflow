package mn.astvision.filterflow.handlers;

import lombok.extern.slf4j.Slf4j;
import mn.astvision.filterflow.model.DbExplainOptions;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

@Slf4j
public class DbExplainHandler {

    private final MongoTemplate mongoTemplate;
    private final Class<?> targetType;
    private final DbExplainOptions options;

    public DbExplainHandler(MongoTemplate template, Class<?> targetType, DbExplainOptions options) {
        this.mongoTemplate = template;
        this.targetType = targetType;
        this.options = options;
    }

    // ------------------ Query Explain ------------------
    public void explainIfNeeded(Query query) {
        if (!options.dbExplainEnabled()) return;

        try {
            Document inner = new Document("find", getCollectionName()).append("filter", query.getQueryObject());

            if (!query.getSortObject().isEmpty()) inner.append("sort", query.getSortObject());
            if (query.getSkip() > 0) inner.append("skip", query.getSkip());
            if (query.getLimit() > 0) inner.append("limit", query.getLimit());

            Document command = new Document("explain", inner).append("verbosity", options.getVerbosity());

            handleExplainOutput(mongoTemplate.getDb().runCommand(command, Document.class), false);

        } catch (Exception e) {
            log.warn("Query explain failed: {}", e.getMessage());
            log.debug("Query explain stack trace", e);
        }
    }

    // ------------------ Aggregation Explain ------------------
    public void explainIfNeeded(Aggregation aggregation) {
        if (!options.dbExplainEnabled()) return;

        try {
            //@formatter:off
            Document inner = new Document("aggregate", getCollectionName())
                    .append("pipeline", aggregation.toPipeline(Aggregation.DEFAULT_CONTEXT))
                    .append("cursor", new Document());

            String verbosity = options.isTiming() || options.isScanStats() || options.isAllPlans()
                    ? "executionStats"
                    : options.getVerbosity();

            Document command = new Document("explain", inner)
                    .append("verbosity", verbosity);
            //@formatter:on


            // Log indexes if requested
            logIndexesIfNeeded();

            handleExplainOutput(mongoTemplate.getDb().runCommand(command, Document.class), true);

        } catch (Exception e) {
            log.warn("Aggregation explain failed: {}", e.getMessage());
            log.debug("Aggregation explain stack trace", e);
        }
    }

    // ------------------ Optional Collection Index Logging ------------------
    public void logIndexesIfNeeded() {
        if (!options.isScanIndexes()) return;

        try {
            List<Document> indexes = mongoTemplate.getCollection(getCollectionName()).listIndexes().into(new java.util.ArrayList<>());

            JsonWriterSettings settings = JsonWriterSettings.builder().indent(true).newLineCharacters("\n").indentCharacters("  ").build();

            log.info("=== Indexes on collection '{}' ===", getCollectionName());
            indexes.forEach(idx -> log.info(idx.toJson(settings)));

        } catch (Exception e) {
            log.warn("Failed to scan indexes for '{}': {}", getCollectionName(), e.getMessage());
            log.debug("Stack trace", e);
        }
    }

    // ------------------ Handle Explain Output ------------------
    @SuppressWarnings("unchecked")
    private void handleExplainOutput(Document explanation, boolean isAggregation) {
        if (explanation == null) {
            log.warn("Explain returned null response");
            return;
        }

        //@formatter:off
        JsonWriterSettings prettySettings = JsonWriterSettings.builder()
                .indent(true)
                .newLineCharacters("\n")
                .indentCharacters("  ")
                .build();
        //@formatter:on


        if (isAggregation) {
            handleAggregationExplain(explanation, prettySettings);
        } else {
            handleQueryExplain(explanation, prettySettings);
        }
    }

    private void handleQueryExplain(Document explanation, JsonWriterSettings prettySettings) {
        Document qp = (Document) explanation.get("queryPlanner");
        Document execStats = (Document) explanation.get("executionStats");

        if (options.isVerbose()) log.info("=== Full explain ===\n{}", explanation.toJson(prettySettings));
        if (options.isQueryPlannerStats())
            log.info("=== Query Planner ===\n{}", qp != null ? qp.toJson(prettySettings) : "null");
        if (options.isIndexStats() && qp != null)
            log.info("=== Winning index ===\n{}",
                    extractIndexName((Document) qp.get("winningPlan")) != null ? extractIndexName((Document) qp.get("winningPlan")) : "none");

        if (execStats != null) {
            if (options.isTiming()) log.info("=== Execution Time (ms) === {}", execStats.get("executionTimeMillis"));
            if (options.isScanStats())
                log.info("=== Scan Stats === Docs examined: {}, Keys examined: {}", execStats.get("totalDocsExamined"), execStats.get("totalKeysExamined"));
            if (options.isAllPlans()) {
                List<Document> allPlans = (List<Document>) execStats.get("allPlansExecution");
                log.info("=== All Candidate Plans ===\n{}", allPlans != null ? allPlans.stream().map(d -> d.toJson(prettySettings)).reduce((a, b) -> a + "\n" + b).orElse("none") : "none");
            }
        }

        if (options.isPlanCacheStats()) safePlanCache(() -> logPlanCache("planCacheListPlans", prettySettings));
        if (options.isPlanCacheHistograms()) safePlanCache(() -> logPlanCache("planCacheListFilters", prettySettings));
    }

    @SuppressWarnings("unchecked")
    private void handleAggregationExplain(Document explanation, JsonWriterSettings prettySettings) {
        // Default minimal: log $match stages if no option is enabled
        boolean anyOptionEnabled = options.isIndexStats() || options.isTiming() || options.isScanStats() || options.isVerbose();

        List<Document> stages = explanation.getList("stages", Document.class);

        if (!anyOptionEnabled) {
            log.info("=== Aggregation $match Stages ===");
            if (stages != null) stages.forEach(stage -> logMatchStage(stage, prettySettings));
            return;
        }

        if (options.isIndexStats() && stages != null) extractAggregationIndexUsage(explanation, prettySettings);

        // executionStats might be nested inside "stages" if top-level execStats is null
        Document execStats = explanation.get("executionStats", Document.class);
        if (execStats == null && stages != null) {
            // try to extract executionStats from first $match stage
            for (Document stage : stages) {
                Document stageExecStats = stage.get("executionStats", Document.class);
                if (stageExecStats != null) {
                    execStats = stageExecStats;
                    break;
                }
            }
        }

        if (execStats != null) {
            if (options.isTiming())
                log.info("=== Aggregation Execution Time (ms) === {}", execStats.get("executionTimeMillis"));
            if (options.isScanStats())
                log.info("=== Aggregation Scan Stats === Docs examined: {}, Keys examined: {}",
                        execStats.get("totalDocsExamined"), execStats.get("totalKeysExamined"));
        }

        if (options.isVerbose()) log.info("=== Full Aggregation Explain ===\n{}", explanation.toJson(prettySettings));
    }

    private void logPlanCache(String commandName, JsonWriterSettings prettySettings) {
        Document statsCmd = new Document(commandName, getCollectionName());
        Document stats = mongoTemplate.getDb().runCommand(statsCmd, Document.class);
        log.info("=== {} ===\n{}", commandName, stats.toJson(prettySettings));
    }

    private void logMatchStage(Document stage, JsonWriterSettings prettySettings) {
        Object matchStageObj = stage.get("$match");
        if (matchStageObj instanceof Document matchStageDoc) log.info(matchStageDoc.toJson(prettySettings));
        else if (matchStageObj != null) log.info(matchStageObj.toString());
    }

    @SuppressWarnings("unchecked")
    private void extractAggregationIndexUsage(Document explanation, JsonWriterSettings prettySettings) {
        Object stagesObj = explanation.get("stages");
        if (!(stagesObj instanceof List<?> stages)) return;

        log.info("=== Aggregation Index Usage ===");

        for (int i = 0; i < stages.size(); i++) {
            Object stageObj = stages.get(i);
            if (!(stageObj instanceof Document stage)) continue;

            // Only handle $match stages
            Object matchObj = stage.get("$match");
            if (!(matchObj instanceof Document matchDoc)) continue;

            // Determine index used from inputStage tree
            String indexUsed = "none";
            Object inputStageObj = stage.get("inputStage");
            if (inputStageObj instanceof Document inputStageDoc) {
                String idx = extractIndexName(inputStageDoc);
                if (idx != null) indexUsed = idx;
            }

            // Log $match content and index
            String matchJson = matchDoc.toJson(prettySettings);
            log.info("Stage {} $match: {}, index used: {}", i + 1, matchJson, indexUsed);
        }
    }

    private void safePlanCache(Runnable command) {
        try {
            command.run();
        } catch (Exception e) {
            log.debug("Plan cache unavailable: {}", e.getMessage());
        }
    }

    private String getCollectionName() {
        return mongoTemplate.getCollectionName(targetType);
    }

    @SuppressWarnings("unchecked")
    private String extractIndexName(Document planStage) {
        if (planStage == null) return null;
        if (planStage.containsKey("indexName")) return planStage.getString("indexName");
        if (planStage.containsKey("inputStage")) return extractIndexName((Document) planStage.get("inputStage"));
        if (planStage.containsKey("inputStages")) for (Document stage : (List<Document>) planStage.get("inputStages")) {
            String idx = extractIndexName(stage);
            if (idx != null) return idx;
        }
        return null;
    }
}
