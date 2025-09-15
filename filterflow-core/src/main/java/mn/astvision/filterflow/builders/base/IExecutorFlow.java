package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.builders.ApplyStepBuilder;
import mn.astvision.filterflow.component.executors.FilterExecutor;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import org.slf4j.Logger;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public interface IExecutorFlow<T> extends IFilterContext<T> {
    List<String> getProjectionFields();

    List<String> getExcludeFields();

    ApplyStepBuilder<T> getApplyStepBuilder();

    BiConsumer<String, Throwable> getGlobalErrorHandler();

    List<AggregationOperation> getAggregationOperations();

    Pageable getPageable();

    Logger getLogger();

    void monitorMemory(String label, Runnable action, FilterOptions opts);

    FilterExecutor.Builder<T> getExecBuilder();

    default Page<T> execute() {
        return executePage(getFilterGroup(), getPageable());
    }

    default Page<T> executePage(List<FilterRequest> filters, Pageable pageable) {
        this.getFilterGroup().addComponent(filters);
        return executePage(getFilterGroup(), pageable);
    }

    default void debug(String message, Object... args) {
        if (!getOptions().isDebug()) return;

        if (args.length > 0) {
            getLogger().info("[DEBUG] {} - {}", message, Arrays.toString(args));
        } else {
            getLogger().info("[DEBUG] {}", message);
        }
    }

    default Page<T> executePage(FilterGroup filters, Pageable pageable) {
        try {
            getOptions().extractFromFilterGroup(filters);

            debug("#######################\n", "options: ", getOptions());
            debug("Executing pageable query with filters: \n" + filters.toSymbolicLogicExpression());

            AtomicReference<Page<T>> page = new AtomicReference<>();

            //@formatter:off
            monitorMemory("execBuilder on class " + getExecBuilder().getTargetType(), () ->
                    page.set(getExecBuilder().withFilters(filters)
                            .withPageable(pageable)
                            .withAggregationOperations(getAggregationOperations())
                            .withExcludeFields(getExcludeFields())
                            .withProjection(getProjectionFields())
                            .withOptions(getOptions())
                            .executePage()), getOptions());
            //@formatter:on

            debug("content size ", page.get().getContent().size(), " total elements ", page.get().getTotalElements());
            monitorMemory("applyStepBuilder on class " + getExecBuilder().getTargetType(),
                    () -> getApplyStepBuilder().execute(page.get().getContent()), getOptions());

            return page.get();
        } catch (Exception ex) {
            if (getGlobalErrorHandler() != null) getGlobalErrorHandler().accept("executePage", ex);
            throw ex;
        }
    }

    default long executeCount(FilterGroup group) {
        try {
            //@formatter:off
            return getExecBuilder().withFilters(group)
                    .withOptions(getOptions())
                    .withAggregationOperations(getAggregationOperations())
                    .executeCount();
            //@formatter:on

        } catch (Exception ex) {
            if (getGlobalErrorHandler() != null) {
                getGlobalErrorHandler().accept("executeCount", ex);
            }
            throw ex;
        }
    }


    default Optional<T> executeSingleResult() {
        return Optional.ofNullable(executeSingleResult(getFilterGroup(), Sort.unsorted()));
    }

    default long count() {
        try {
            //@formatter:off
            return getExecBuilder().withFilters(getFilterGroup())
                    .withOptions(getOptions())
                    .withAggregationOperations(getAggregationOperations())
                    .executeCount();
            //@formatter:on

        } catch (Exception ex) {
            if (getGlobalErrorHandler() != null) {
                getGlobalErrorHandler().accept("executeCount", ex);
            }
            throw ex;
        }
    }

    default T executeSingleResult(FilterGroup filters, Sort sort) {
        getOptions().extractFromFilterGroup(filters);
        //@formatter:off
        return getExecBuilder().withFilters(filters)
                .withAggregationOperations(getAggregationOperations())
                .withExcludeFields(getExcludeFields())
                .withProjection(getProjectionFields())
                .withOptions(getOptions())
                .executeSingleResult(sort);
        //@formatter:on
    }

    default boolean exists(FilterGroup filters) {
        getOptions().extractFromFilterGroup(filters);

        //@formatter:off
        return getExecBuilder().withFilters(filters)
                .withAggregationOperations(getAggregationOperations())
                .withOptions(getOptions())
                .exists();
        //@formatter:on
    }


    default List<T> executeList(List<FilterRequest> filters) {
        try {
            FilterGroup fg = new FilterGroup();
            fg.addComponent(filters);
            getOptions().extractFromFilterGroup(fg);

            //@formatter:off
            List<T> list = getExecBuilder().withFilters(filters)
                    .withOptions(getOptions())
                    .executeList();
            //@formatter:on

            getApplyStepBuilder().execute(list);

            return list;
        } catch (Exception ex) {
            if (getGlobalErrorHandler() != null) {
                getGlobalErrorHandler().accept("executeList", ex);
            }
            throw ex;
        }
    }

    // ---- Helpers to reduce duplication ----
    private <R> R runWithHandling(String opName, Supplier<R> action) {
        try {
            return action.get();
        } catch (Exception ex) {
            if (getGlobalErrorHandler() != null) getGlobalErrorHandler().accept(opName, ex);
            throw ex;
        }
    }

    private <R> R timed(String label, Supplier<R> action) {
        final Object[] result = new Object[1];
        monitorMemory(label + " on class " + getTargetType(), () -> result[0] = action.get(), getOptions());
        @SuppressWarnings("unchecked")
        R casted = (R) result[0];
        return casted;
    }

    private void timed(String label, Runnable action) {
        monitorMemory(label + " on class " + getTargetType(), action, getOptions());
    }

}


