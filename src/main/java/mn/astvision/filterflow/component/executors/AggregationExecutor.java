package mn.astvision.filterflow.component.executors;

import mn.astvision.commontools.monitoring.MemoryUtils;
import mn.astvision.filterflow.component.abstraction.AbstractMongoExecutor;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import org.bson.Document;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

public class AggregationExecutor<T> extends AbstractMongoExecutor<T> {
    private static final String FIELD_TOTAL = "aggregateTotalCount";
    private final String collectionName;

    private AggregationExecutor(Builder<T> builder) {
        super(builder.mongoTemplate, builder.filterOptions, builder.targetType);
        this.collectionName = builder.collectionName;
    }

    public static <T> Builder<T> forType(Class<T> targetType) {
        return new Builder<T>().withTargetType(targetType);
    }

    public static <T> Builder<T> fromFBuilder(FilterExecutor.Builder<T> tBuilder) {
        //@formatter:off
        return new Builder<T>()
                .withTargetType(tBuilder.getTargetType())
                .withMongoTemplate(tBuilder.getMongoTemplate())
                .filterOptions(tBuilder.getFilterOptions())
                .operations(tBuilder.getAggregationOperations())
                .projectOperations(tBuilder.getProjectOperations())
                .filters(tBuilder.getFilterGroup())
                .page(tBuilder.getPageable());
        //@formatter:on
    }

    public Page<T> executePage(FilterGroup filters, List<AggregationOperation> operations, List<AggregationOperation> projOps, Pageable pageable) {
        try {
            Instant start = Instant.now();
            List<AggregationOperation> pipeline = buildPipeline(filters, operations, projOps);

            if (pageable.isUnpaged()) {
                return executeUnpaged(pipeline, pageable, start);
            }

            Sort sort = pageable.getSort();
            if (!sort.isUnsorted() && !sort.isEmpty()) {
                pipeline.add(Aggregation.sort(pageable.getSort()));
            }
            pipeline.add(Aggregation.skip(pageable.getOffset()));
            pipeline.add(Aggregation.limit(pageable.getPageSize()));

            Aggregation aggregation = Aggregation.newAggregation(pipeline);
            debug("Executing paged aggregation on " + getCollectionName());
            debug("aggregation: ", aggregation);
            dbExplainHandler.explainIfNeeded(aggregation);


            try (Stream<T> stream = mongoTemplate.aggregateStream(aggregation, getCollectionName(), targetType)) {
                List<T> results = new ArrayList<>();
                stream.forEach(results::add);
                long total = options.isSkipCount() ? -1L : executeCount(pipeline);
                logTimeIfNeeded("executePage", start);
                return new PageImpl<>(results, pageable, total >= 0 ? total : results.size());
            }

        } catch (FilterException fe) {
            throw fe;
        } catch (Exception e) {
            log.error("Error executing paged aggregation for {}: {}", targetType.getSimpleName(), e.getMessage(), e);
            return Page.empty(pageable);
        }
    }


    private Page<T> executeUnpaged(List<AggregationOperation> pipeline, Pageable pageable, Instant start) {
        Aggregation aggregation = Aggregation.newAggregation(pipeline);
        debug("Executing unpaged aggregation on " + getCollectionName());
        debug("aggregation: ", aggregation);

        try (Stream<T> stream = mongoTemplate.aggregateStream(aggregation, getCollectionName(), targetType)) {
            List<T> results = new ArrayList<>();
            stream.forEach(results::add);
            logTimeIfNeeded("executeUnpaged", start);
            return new PageImpl<>(results, pageable, results.size());
        }
    }

    private void logTimeIfNeeded(String method, Instant start) {
        if (options.isExecutionStatLoggerEnabled()) {
            log.info("[LOG EXEC TIME] AggregationExecutor.{} executed in {} ms", method, Duration.between(start, Instant.now()).toMillis());
        }
    }

    private String getCollectionName() {
        return (collectionName != null) ? collectionName : mongoTemplate.getCollectionName(targetType);
    }

    public T executeSingle(FilterGroup filters, List<AggregationOperation> ops, List<AggregationOperation> projectOps, Sort sort) {
        Instant start = Instant.now();
        List<AggregationOperation> pipeline = buildPipeline(filters, ops);

        boolean hasProjection = CollectionUtils.isEmpty(projectOps);

        if (sort != null && sort.isSorted()) {
            pipeline.add(Aggregation.sort(sort));

            if (!hasProjection) {
                debug("added project ops AFTER sort, size: {}", projectOps.size());
                pipeline.addAll(projectOps);
            }
        } else {
            if (!hasProjection) {
                debug("added project ops size: {}", projectOps.size());
                pipeline.addAll(projectOps);
            }
        }

        pipeline.add(Aggregation.limit(1));

        Aggregation aggregation = Aggregation.newAggregation(pipeline);
        debug("Executing single aggregation for {}", targetType.getSimpleName());

        try {
            AggregationResults<T> result = mongoTemplate.aggregate(aggregation, getCollectionName(), targetType);
            logTimeIfNeeded("executeSingle", start);
            return result.getUniqueMappedResult();
        } catch (Exception e) {
            log.error("Error executing single aggregation for {}: {}", targetType.getSimpleName(), e.getMessage(), e);
            throw new FilterException("Failed to execute single aggregation", e);
        }
    }

    private long executeCount(List<AggregationOperation> pipeline) {
        try {
            List<AggregationOperation> countPipeline = removeSkipAndLimit(pipeline);
            countPipeline = new ArrayList<>(countPipeline);
            countPipeline.add(Aggregation.count().as(FIELD_TOTAL));

            debug("Executing count aggregation on " + getCollectionName());
            Instant start = Instant.now();

            try (Stream<Document> stream = mongoTemplate.aggregateStream(
                    Aggregation.newAggregation(countPipeline),
                    getCollectionName(),
                    Document.class)) {

                long count = stream.findFirst()
                        .map(doc -> ((Number) doc.get(FIELD_TOTAL)).longValue())
                        .orElse(0L);

                logTimeIfNeeded("executeCount", start);
                return count;
            }
        } catch (Exception e) {
            log.error("Error executing count aggregation for {}: {}", targetType.getSimpleName(), e.getMessage(), e);
            return 0L;
        }
    }


    private List<AggregationOperation> buildPipeline(FilterGroup filters, List<AggregationOperation> ops, List<AggregationOperation> projectOps) {
        List<AggregationOperation> pipeline = buildPipeline(filters, ops);
        if (CollectionUtils.isEmpty(projectOps)) return pipeline;
        debug("added project ops on build pipeline size: ", projectOps.size());
        pipeline.addAll(projectOps);

        return pipeline;
    }

    private List<AggregationOperation> buildPipeline(FilterGroup filters, List<AggregationOperation> ops) {
        List<AggregationOperation> pipeline = new ArrayList<>();

        try {
            if (!CollectionUtils.isEmpty(ops)) {
                debug("added ops on build pipeline size: ", ops.size());
                pipeline.addAll(ops);
            }

            if (filters != null && !filters.getComponents().isEmpty()) {
                Criteria criteria = buildCriteria(filters);
                debug("Built criteria: ", criteria.getCriteriaObject());
                pipeline.add(Aggregation.match(criteria));
            }

        } catch (FilterException fe) {
            throw fe;
        } catch (Exception e) {
            log.error("Error building aggregation pipeline for {}: {}", targetType.getSimpleName(), e.getMessage(), e);
        }

        return pipeline;
    }


    public Aggregation buildSuggestionAggregation(String field, FilterGroup filters, int limit, Sort.Direction direction) {
        List<AggregationOperation> pipeline = new ArrayList<>();

        Criteria criteria = buildCriteria(filters);

        pipeline.add(Aggregation.match(criteria));
        pipeline.add(Aggregation.group(field).first(field).as("value"));
        pipeline.add(Aggregation.sort(direction != null ? direction : Sort.Direction.ASC, "value"));
        if (limit > 0) {
            pipeline.add(Aggregation.limit(limit));
        }

        return Aggregation.newAggregation(pipeline);
    }

    private void handleException(String method, Exception e) {
        log.error("AggregationExecutor.{} failed: {}", method, e.getMessage(), e);
        if (options.isFailFast()) {
            throw new RuntimeException("Aggregation execution failed in method: " + method, e);
        }
    }

    private List<AggregationOperation> removeSkipAndLimit(List<AggregationOperation> ops) {
        if (CollectionUtils.isEmpty(ops)) return List.of();
        List<AggregationOperation> cleaned = new ArrayList<>();
        for (AggregationOperation op : ops) {
            if (!(op instanceof SkipOperation) && !(op instanceof LimitOperation)) {
                cleaned.add(op);
            }
        }
        return cleaned;
    }

    private boolean exists(FilterGroup filters, List<AggregationOperation> aggregationOperations) {
        Instant start = Instant.now();

        List<AggregationOperation> pipelines = buildPipeline(filters, aggregationOperations);
        //@formatter:off
        List<AggregationOperation> ops = Stream.concat(
                pipelines.stream(),
                Stream.of(
                        Aggregation.limit(1),
                        Aggregation.project("_id")
                )
        ).toList();
        //@formatter:on

        Aggregation aggregation = Aggregation.newAggregation(ops);

        debug("Executing exits aggregation on " + getCollectionName());
        debug("aggregation: ", aggregation);

        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, targetType, Document.class);

        boolean exists = results.getUniqueMappedResult() != null;
        logTimeIfNeeded("exists", start);

        return exists;
    }

    public static class Builder<T> {
        private MongoTemplate mongoTemplate;
        private Class<T> targetType;
        private String collectionName;
        private FilterGroup filters;
        private Pageable pageable;
        private List<AggregationOperation> operations;
        private List<AggregationOperation> projectOperations;
        private FilterOptions filterOptions = FilterOptions.defaults();

        private Builder<T> withTargetType(Class<T> tClass) {
            this.targetType = tClass;
            return this;
        }

        public Builder<T> withMongoTemplate(MongoTemplate template) {
            this.mongoTemplate = template;
            return this;
        }

        public Builder<T> page(Pageable val) {
            this.pageable = val;
            return this;
        }

        public Builder<T> debug(boolean val) {
            this.filterOptions.setDebug(val);
            return this;
        }

        public Builder<T> collectionName(String val) {
            this.collectionName = val;
            return this;
        }

        public Builder<T> filters(FilterGroup val) {
            if (val == null) return this;

            this.filters = val;
            return this;
        }

        public Builder<T> operations(List<AggregationOperation> val) {
            if (val == null) return this;

            this.operations = val;
            return this;
        }

        public AggregationExecutor<T> build() {
            if (mongoTemplate == null) {
                throw new FilterException("MongoTemplate must be provided");
            }
            if (targetType == null) {
                throw new FilterException("Target type must be provided");
            }

            return new AggregationExecutor<>(this);
        }

        public Page<T> execute() {
            if (pageable == null) this.pageable = Pageable.unpaged();
            AggregationExecutor<T> build = build();
            AtomicReference<Page<T>> page = new AtomicReference<>();

            MemoryUtils.monitorPerformance("executing aggregation", () -> {
                page.set(build.executePage(filters, operations, projectOperations, pageable));
            }, filterOptions.getMemoryThreshholdPercent());

            return page.get();
        }

        public long executeCount() {
            if (pageable == null) this.pageable = Pageable.unpaged();

            AggregationExecutor<T> build = build();
            List<AggregationOperation> ops = build.buildPipeline(filters, operations);
            return build.executeCount(ops);
        }


        public Builder<T> filterOptions(FilterOptions opt) {
            this.filterOptions = opt;
            return this;
        }

        public Builder<T> projectOperations(List<AggregationOperation> projectOperations) {
            if (projectOperations == null) return this;

            for (AggregationOperation op : projectOperations) {
                if (!(op instanceof ProjectionOperation)) {
                    throw new FilterException("Invalid project operation detected: " + op.getClass().getSimpleName() + ". Only ProjectionOperation is allowed.");
                }
            }

            this.projectOperations = projectOperations;

            return this;
        }

        public T executeSingleResult(Sort sort) {
            return build().executeSingle(filters, operations, projectOperations, sort);
        }

        public boolean exists() {
            return build().exists(filters, operations);
        }
    }
}
