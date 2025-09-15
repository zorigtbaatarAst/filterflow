package mn.astvision.filterflow.component.executors;

import lombok.Getter;
import mn.astvision.commontools.monitoring.MemoryUtils;
import mn.astvision.filterflow.builders.ProjectionBuilder;
import mn.astvision.filterflow.component.FilterExecutionStatsHolder;
import mn.astvision.filterflow.component.abstraction.AbstractMongoExecutor;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.util.VirtualFieldResolverUtil;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */


public class FilterExecutor<T> extends AbstractMongoExecutor<T> {
    private static final Map<Class<?>, Set<String>> ENTITY_FIELDS_CACHE = new ConcurrentHashMap<>();
    private final FilterGroup filterGroup;
    private final Pageable pageable;

    private FilterExecutor(Builder<T> builder) {
        super(builder.mongoTemplate,
                builder.filterOptions != null ? builder.filterOptions : FilterOptions.defaults(),
                Objects.requireNonNull(builder.targetType, "Target type must not be null"));

        this.filterGroup = builder.filterGroup;
        this.pageable = builder.pageable;

        if (mongoTemplate == null) {
            throw new FilterException("MongoTemplate must be provided");
        }
    }

    public static <T> Builder<T> forType(Class<T> targetType) {
        return new Builder<>(targetType);
    }

    public Page<T> executePage() {
        Assert.notNull(pageable, () -> {
            throw new FilterException("Pageable must not be null", targetType,
                    "Call withPageable(Pageable) before execution");
        });

        try {
            Criteria criteria = buildCriteria(filterGroup);
            Query queryWithPage = Query.query(criteria).with(pageable);

            debug("executing page, criteria: {}", criteria.getCriteriaObject());
            dbExplainHandler.explainIfNeeded(queryWithPage);

            try (Stream<T> stream = mongoTemplate.stream(queryWithPage, targetType)) {
                List<T> content = stream.toList();
                LongSupplier countSupplier = () -> mongoTemplate.count(Query.query(criteria), targetType);
                return PageableExecutionUtils.getPage(content, pageable, countSupplier);
            }
        } catch (FilterException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Failed to execute query", ex);
            throw new FilterException("Failed to execute query", ex);
        }
    }

    private long executeCount() {
        Query countQuery = Query.query(buildCriteria(filterGroup));
        return mongoTemplate.count(countQuery, targetType);
    }

    public List<T> executeList() {
        Criteria criteria = buildCriteria(filterGroup);
        debug("Built criteria: {}", criteria.getCriteriaObject());

        return mongoTemplate.find(Query.query(criteria), targetType);
    }

    public T executeSingleResult(Sort sort) {
        try {
            validateSortFields(sort);
            Criteria criteria = buildCriteria(filterGroup);
            Query query = new Query(criteria).with(sort).limit(1);
            debug("executing single result, criteria: {}", criteria.getCriteriaObject());

            return mongoTemplate.findOne(query, targetType);
        } catch (Exception ex) {
            String msg = String.format("Failed to execute sorted single result query on '%s': %s",
                    targetType.getSimpleName(), ex.getMessage());
            throw new FilterException(msg, ex);
        }
    }

    private Set<String> getCachedFields() {
        return ENTITY_FIELDS_CACHE.computeIfAbsent(targetType,
                t -> getAllMappedFieldNames(getPersistentEntityOrThrow()));
    }

    private void validateSortFields(Sort sort) {
        if (sort == null || sort.isUnsorted()) return;

        Set<String> availableFields = getCachedFields();
        for (Sort.Order order : sort) {
            String property = order.getProperty();
            if (!availableFields.contains(property)) {
                throw new FilterException(
                        String.format("Invalid sort field '%s' for type '%s'. Allowed fields: %s",
                                property, targetType.getSimpleName(), availableFields));
            }
        }
    }

    private boolean exists() {
        try {
            Criteria criteria = buildCriteria(filterGroup);
            debug("executing exists, criteria: {}", criteria.getCriteriaObject());
            return mongoTemplate.exists(new Query(criteria), targetType);
        } catch (Exception ex) {
            log.error("Failed to execute query", ex);
            throw new FilterException("Failed to execute query", ex);
        }
    }

    @Getter
    public static class Builder<T> {
        private final List<AggregationOperation> aggregationOperations = new ArrayList<>();
        private final List<AggregationOperation> projectOperations = new ArrayList<>();
        private final Class<T> targetType;
        private final ProjectionBuilder<T> tProjectionBuilder;
        private FilterOptions filterOptions;
        private FilterGroup filterGroup;
        private MongoTemplate mongoTemplate;
        private Pageable pageable;

        public Builder(Class<T> targetType) {
            this.targetType = targetType;
            this.tProjectionBuilder = ProjectionBuilder.ofType(targetType);
        }

        public Builder<T> withAggregationOperations(List<AggregationOperation> operations) {
            if (operations != null) this.aggregationOperations.addAll(operations);

            return this;
        }

        public Builder<T> withProjection(String... fieldNames) {
            if (fieldNames.length == 0) return this;

            tProjectionBuilder.withFields(List.of(fieldNames)).set(this.projectOperations);

            return this;
        }

        public Builder<T> withProjection(List<String> fieldNames) {
            if (fieldNames == null || fieldNames.isEmpty()) return this;

            tProjectionBuilder.withFields(fieldNames).set(this.projectOperations);

            return this;
        }

        public Builder<T> withExcludeFields(List<String> fieldNames) {
            if (fieldNames == null || fieldNames.isEmpty()) return this;

            tProjectionBuilder.withExcludeFields(fieldNames).set(this.projectOperations);
            return this;
        }

        public <P> Builder<T> withAutoProjection(Class<P> projectionType) {
            //@formatter:off
            tProjectionBuilder
                    .withTemplate(this.mongoTemplate)
                    .withAutoProjection(projectionType)
                    .set(projectOperations);
            //@formatter:on

            return this;
        }

        public Builder<T> withOptions(FilterOptions options) {
            this.filterOptions = options != null ? options : FilterOptions.defaults();
            if (filterOptions.isResolveVF()) resolveVirtualField();
            withProjection(filterOptions.getProject());
            withExcludeFields(filterOptions.getExclude());
            return this;
        }

        private void resolveVirtualField() {
            boolean exits = VirtualFieldResolverUtil.hasVirtualFields(targetType);
            if (!exits) return;

            List<AggregationOperation> ops = VirtualFieldResolverUtil.resolve(targetType, filterOptions.isDebug());
            this.aggregationOperations.addAll(ops);
        }


        public Builder<T> withMongoTemplate(MongoTemplate mongoTemplate) {
            this.mongoTemplate = Objects.requireNonNull(mongoTemplate, "MongoTemplate must not be null");
            return this;
        }

        public Builder<T> withFilters(List<FilterRequest> filters) {
            if (filters == null || filters.isEmpty()) {
                return this;
            }

            FilterGroup fg = new FilterGroup();
            fg.addComponent(filters);
            this.filterGroup = fg;
            return this;
        }

        public Builder<T> withFilters(FilterGroup filterGroup) {
            this.filterGroup = filterGroup;
            return this;
        }

        public Builder<T> withPageable(Pageable pageable) {
            this.pageable = pageable;
            return this;
        }

        public FilterExecutor<T> build() {
            if (mongoTemplate == null) {
                throw new FilterException("MongoTemplate must be provided");
            }
            if (targetType == null) {
                throw new FilterException("Target type must be provided");
            }

            return new FilterExecutor<>(this);
        }


        public Page<T> executePage() {
            if (pageable == null) this.pageable = Pageable.unpaged();
            Page<T> result;

            long start = System.nanoTime();

            if (aggregationOperations.isEmpty() && projectOperations.isEmpty()) {
                AtomicReference<Page<T>> finalResult = new AtomicReference<>();
                MemoryUtils.monitorPerformance("executing aggregation", () -> {
                    finalResult.set(build().executePage());
                }, filterOptions.getMemoryThreshholdPercent());

                result = finalResult.get();
            } else {
                result = AggregationExecutor.fromFBuilder(this).execute();
            }

            recordStats(start, result.getContent().size(), !aggregationOperations.isEmpty());

            return result;
        }

        public List<T> executeList() {
            long start = System.nanoTime();
            List<T> result = build().executeList();
            recordStats(start, result.size(), false);


            return result;
        }

        public <V> Set<V> executeSuggestionByFieldWithType(String field) {
            long start = System.nanoTime();
            if (filterOptions == null)
                filterOptions = FilterOptions.defaults();

            filterOptions.extractFromFilterGroup(filterGroup);

            SuggestionExecutor<T> suggestionExecutor = new SuggestionExecutor<>(mongoTemplate, filterOptions, targetType);
            Set<V> result = suggestionExecutor.suggestByFieldWithType(field, 0, 0, Sort.unsorted(), filterGroup);

            recordStats(start, result.size(), true);

            //@formatter:on

            return result;
        }

        public Page<Object> executeSuggestionByField(String field, Pageable pageable) {
            long start = System.nanoTime();
            if (filterOptions == null)
                filterOptions = FilterOptions.defaults();

            filterOptions.extractFromFilterGroup(filterGroup);

            SuggestionExecutor<T> suggestionExecutor = new SuggestionExecutor<>(mongoTemplate, filterOptions, targetType);


            Page<Object> result = suggestionExecutor.suggestByField(field, pageable, filterGroup);
            recordStats(start, result.getContent().size(), true);

            return result;
        }

        public Set<Object> executeSuggestionByField(String field) {
            long start = System.nanoTime();
            if (filterOptions == null)
                filterOptions = FilterOptions.defaults();

            filterOptions.extractFromFilterGroup(filterGroup);

            SuggestionExecutor<T> suggestionExecutor = new SuggestionExecutor<>(mongoTemplate, filterOptions, targetType);
            Set<Object> result = suggestionExecutor.suggestByField(field, 0, 0, Sort.unsorted(), filterGroup);

            recordStats(start, result.size(), true);

            //@formatter:on

            return result;
        }

        public long executeCount() {
            long result;

            if (aggregationOperations.isEmpty() && projectOperations.isEmpty()) {
                result = build().executeCount();
            } else {
                result = AggregationExecutor.fromFBuilder(this).executeCount();
            }

            return result;
        }

        public T executeSingleResult(Sort sort) {
            if (sort == null) sort = Sort.unsorted();
            T result;

            if (aggregationOperations.isEmpty() && projectOperations.isEmpty()) {
                result = build().executeSingleResult(sort);
            } else {
                result = AggregationExecutor.fromFBuilder(this).executeSingleResult(sort);
            }

            return result;
        }

        public T executeSingleResult() {
            return executeSingleResult(Sort.unsorted());
        }

        private void recordStats(long start, long size, boolean usingAggregation) {
            int fCount = filterGroup != null ? filterGroup.count() : 0;
            FilterExecutionStatsHolder.createStats(start, size, usingAggregation, fCount, pageable);
        }

        public boolean exists() {
            boolean result;

            if (aggregationOperations.isEmpty() && projectOperations.isEmpty()) {
                result = build().exists();
            } else {
                result = AggregationExecutor.fromFBuilder(this).exists();
            }

            return result;
        }
    }
}
