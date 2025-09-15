package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.component.executors.SummaryExecutor;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ISummaryFlow<T, SELF> extends IFilterContext<T> {
    Map<String, Set<SummaryExecutor.AggregateOp>> getSummaryOps();

    SELF self(); // should return the implementing object

    // --- internal helper method ---
    default SummaryExecutor<T> summaryExecutor() {
        return getFactory().forSummaryType(getTargetType(), getOptions());
    }

    // --- Aggregate Methods ---
    default BigDecimal sum(String field) {
        return summaryExecutor().execute(field, SummaryExecutor.AggregateOp.SUM, getFilterGroup());
    }

    default BigDecimal min(String field) {
        return summaryExecutor().execute(field, SummaryExecutor.AggregateOp.MIN, getFilterGroup());
    }

    default BigDecimal max(String field) {
        return summaryExecutor().execute(field, SummaryExecutor.AggregateOp.MAX, getFilterGroup());
    }

    default BigDecimal avg(String field) {
        return summaryExecutor().execute(field, SummaryExecutor.AggregateOp.AVG, getFilterGroup());
    }

    default Map<String, Map<String, BigDecimal>> summarize(Map<String, Set<SummaryExecutor.AggregateOp>> fieldOps) {
        return summaryExecutor().executeMany(fieldOps, getFilterGroup());
    }

    default Map<String, Map<String, BigDecimal>> summarizeFromString(Map<String, Set<String>> fieldOps) {
        Map<String, Set<SummaryExecutor.AggregateOp>> converted = SummaryExecutor.convertFieldOps(fieldOps, getOptions().isDebug());
        return summaryExecutor().executeMany(converted, getFilterGroup());
    }

    // --- Chainable Summary Config ---
    default SELF withSummaries(Map<String, Set<SummaryExecutor.AggregateOp>> fieldOps) {
        Map<String, Set<SummaryExecutor.AggregateOp>> ops = getSummaryOps();
        if (ops == null) throw new IllegalStateException("summaryOps must be initialized in implementing class");
        ops.putAll(fieldOps);
        return self();
    }

    default SELF withSummary(String field, SummaryExecutor.AggregateOp op) {
        Map<String, Set<SummaryExecutor.AggregateOp>> ops = getSummaryOps();
        if (ops == null) throw new IllegalStateException("summaryOps must be initialized in implementing class");
        ops.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(op);
        return self();
    }

    default SELF withSummary(String... fields) {
        for (String field : fields) {
            withSummary(field, SummaryExecutor.AggregateOp.SUM);
        }
        return self();
    }

    default SELF withSummary(List<String> fields) {
        for (String field : fields) {
            withSummary(field, SummaryExecutor.AggregateOp.SUM);
        }
        return self();
    }
}
