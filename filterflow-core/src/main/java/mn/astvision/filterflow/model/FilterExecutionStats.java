package mn.astvision.filterflow.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Objects;

@Data
@Builder
public final class FilterExecutionStats {
    private final long durationMillis;
    private final int filterCount;
    private final boolean isPaged;
    private final int pageSize;
    private final int pageNumber;
    private final long resultCount;
    private final Instant startTime;
    private final Instant endTime;
    private final boolean usedAggregation;

    public static FilterExecutionStats create(long start, int filterCount, Pageable pageable, long resultCount, boolean usedAggregation) {
        long durationNano = (System.nanoTime() - start) / 1_000_000;
        long durationMillis = durationNano / 1_000_000;

        Instant endTime = Instant.now();
        Instant startTime = endTime.minusMillis(durationMillis);

        boolean isPaged = pageable != null && pageable.isPaged();
        int pageSize = isPaged ? pageable.getPageSize() : (int) resultCount;
        int pageNumber = isPaged ? pageable.getPageNumber() : 0;

        //@formatter:off
        return FilterExecutionStats.builder()
                .durationMillis(durationMillis)
                .filterCount(filterCount)
                .isPaged(isPaged)
                .pageSize(pageSize)
                .pageNumber(pageNumber)
                .resultCount(resultCount)
                .startTime(startTime)
                .endTime(endTime)
                .usedAggregation(usedAggregation)
                .build();
        //@formatter:on

    }

    @Override
    public String toString() {
        return "FilterExecutionStats{" + "durationMillis=" + durationMillis + ", filterCount=" + filterCount + ", isPaged=" + isPaged + ", pageSize=" + pageSize + ", pageNumber=" + pageNumber + ", resultCount=" + resultCount + ", startTime=" + startTime + ", endTime=" + endTime + ", usedAggregation=" + usedAggregation + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (FilterExecutionStats) obj;
        return this.durationMillis == that.durationMillis && this.filterCount == that.filterCount && this.isPaged == that.isPaged && this.pageSize == that.pageSize && this.pageNumber == that.pageNumber && this.resultCount == that.resultCount && Objects.equals(this.startTime, that.startTime) && Objects.equals(this.endTime, that.endTime) && this.usedAggregation == that.usedAggregation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationMillis, filterCount, isPaged, pageSize, pageNumber, resultCount, startTime, endTime, usedAggregation);
    }

}
