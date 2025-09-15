package mn.astvision.filterflow.builders.base;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import mn.astvision.commontools.monitoring.MemoryUtils;
import mn.astvision.filterflow.builders.ApplyStepBuilder;
import mn.astvision.filterflow.builders.FilterBuilder;
import mn.astvision.filterflow.builders.SuggestionBuilder;
import mn.astvision.filterflow.component.FilterContextHolder;
import mn.astvision.filterflow.component.executors.FilterExecutor;
import mn.astvision.filterflow.component.executors.SummaryExecutor;
import mn.astvision.filterflow.component.factory.FilterExecutorFactory;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import org.slf4j.Logger;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Getter
public abstract class BaseFilterFlowBuilder<T, SELF extends BaseFilterFlowBuilder<T, SELF>>
        //@formatter:off
        implements
            ISummaryFlow<T, SELF>,
            IApplyStepFlow<T, SELF>,
            IOptionFlow<SELF>,
            IFieldSelectionFlow<SELF>,
            IExecutorFlow<T>
        //@formatter:on
{
    protected final FilterExecutor.Builder<T> execBuilder;
    protected final ApplyStepBuilder<T> applyStepBuilder;

    protected FilterOptions options;
    protected FilterGroup filterGroup;
    protected List<String> projectionFields = new ArrayList<>();
    protected List<String> excludeFields = new ArrayList<>();
    protected List<AggregationOperation> aggregationOperations;
    protected Pageable pageable;

    protected BiConsumer<String, Throwable> globalErrorHandler;
    protected Map<String, Set<SummaryExecutor.AggregateOp>> summaryOps = new HashMap<>();

    public BaseFilterFlowBuilder(FilterExecutor.Builder<T> builder) {
        this.execBuilder = builder;
        this.options = builder.getFilterOptions();
        if (options == null) {
            options = FilterOptions.defaults();
        }
        this.filterGroup = builder.getFilterGroup();
        if (filterGroup == null)
            filterGroup = new FilterGroup();
        this.aggregationOperations = builder.getAggregationOperations();
        this.pageable = Pageable.unpaged();
        this.applyStepBuilder = new ApplyStepBuilder<>(options);
    }

    // --- Chainable methods ---
    @SuppressWarnings("unchecked")
    public SELF self() {
        return (SELF) this;
    }

    public SELF withAggregationOperations(List<AggregationOperation> operations) {
        if (operations != null) this.aggregationOperations.addAll(operations);
        return self();
    }

    public SELF filterBuilder(Consumer<FilterBuilder<T>> builderConsumer) {
        FilterBuilder<T> builder = FilterBuilder.ofType(this.execBuilder.getTargetType());
        builderConsumer.accept(builder);
        this.filterGroup.addComponent(builder.build());
        return self();
    }

    public SELF withFilter(List<FilterRequest> filters) {
        if (filters != null) {
            this.filterGroup.clear();
            this.filterGroup.addComponent(filters);
        }
        return self();
    }

    public SELF withFilter(FilterGroup filters) {
        if (filters != null) {
            this.filterGroup.clear();
            this.filterGroup.addComponent(filters);
        }
        return self();
    }

    public SELF withOption(FilterOptions options) {
        if (options != null) {
            this.options = options;
            this.execBuilder.withOptions(options);
        }
        return self();
    }

    public SELF addFilter(List<FilterRequest> filters) {
        if (filters == null) return self();
        filterGroup.addComponent(filters);
        return self();
    }

    public SELF addFilter(FilterRequest filters) {
        if (filters == null) return self();
        filterGroup.addComponent(filters);
        return self();
    }

    public SELF withPage(Pageable pageable) {
        this.pageable = pageable;
        return self();
    }

    public SELF withSort(Sort sort) {
        if (sort == null || sort.isUnsorted()) {
            return self();
        }

        if (this.pageable == null || this.pageable.isUnpaged()) {
            this.pageable = PageRequest.of(0, Integer.MAX_VALUE, sort); // fallback: unpaged but sorted
        } else {
            this.pageable = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), sort);
        }

        return self();
    }

    public SELF onError(BiConsumer<String, Throwable> handler) {
        this.globalErrorHandler = handler;
        this.getApplyStepBuilder().onError(handler);
        return self();
    }

    public SELF withLimit(int limit) {
        this.pageable = Pageable.ofSize(limit);
        return self();
    }


    public SuggestionBuilder<T> suggestion(String field) {
        return new SuggestionBuilder<T>(field, filterGroup, execBuilder);
    }

    public <B extends BaseFilterFlowBuilder<T, B>> B convertBuilder(Function<FilterExecutor.Builder<T>, B> builderSupplier) {
        B newBuilder = builderSupplier.apply(getFactory().forType(this.getTargetType()));

        newBuilder.withFilter(this.getFilterGroup());
        newBuilder.getProjectionFields().addAll(this.getProjectionFields());
        newBuilder.getExcludeFields().addAll(this.getExcludeFields());
        newBuilder.withPage(this.getPageable());
        newBuilder.withAggregationOperations(this.getAggregationOperations());
        newBuilder.withOption(this.getOptions());

        return newBuilder;
    }

    @Override
    public FilterExecutorFactory getFactory() {
        return FilterContextHolder.getBean(FilterExecutorFactory.class);
    }

    @Override
    public Class<T> getTargetType() {
        return execBuilder.getTargetType();
    }

    @Override
    public void monitorMemory(String label, Runnable action, FilterOptions opts) {
        MemoryUtils.monitorMemory(label, action, opts.getMemoryThreshholdPercent());
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
