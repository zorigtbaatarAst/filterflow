package mn.astvision.filterflow.component;

import mn.astvision.filterflow.model.FilterExecutionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;

/**
 * @author zorigtbaatar
 */

public class FilterExecutionStatsHolder {
    private static final ThreadLocal<FilterExecutionStats> statsHolder = new ThreadLocal<>();
    private static final Logger log = LoggerFactory.getLogger(FilterExecutionStatsHolder.class);

    public static FilterExecutionStats getStats() {
        return statsHolder.get();
    }

    public static void setStats(FilterExecutionStats stats) {
        statsHolder.set(stats);
    }

    public static String generateFilterStatsString() {
        FilterExecutionStats filterStats = getStats();
        //@formatter:off
        return  (filterStats != null)
                ? String.format("""
                                🧾 Filter Execution Stats:
                                  ⏱️ Duration: %d ms
                                  🔢 Filters: %d
                                  📄 Page Size: %s
                                  📊 Total Elements: %d
                                """,
                filterStats.getDurationMillis(),
                filterStats.getFilterCount(),
                filterStats.getPageSize() != 0 ? filterStats.getPageSize() : "N/A",
                filterStats.getResultCount()
        )
                : "📭 No FilterExecutionStats found in context.";

        //@formatter:on
    }

    public static void clear() {
        statsHolder.remove();
    }

    public static void printStatLog() {
        log.info(generateFilterStatsString());
    }

    public static void createStats(long start, long size, boolean usingAggregation, int filterCount, Pageable pageable) {
        FilterExecutionStats filterExecutionStats = FilterExecutionStats.create(start, filterCount, pageable, size, usingAggregation);
        setStats(filterExecutionStats);
    }
}
