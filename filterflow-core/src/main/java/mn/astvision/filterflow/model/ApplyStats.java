package mn.astvision.filterflow.model;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Getter
public class ApplyStats {
    private final static Logger logger = LoggerFactory.getLogger(ApplyStats.class);
    private final List<String> failedSteps = new ArrayList<>();
    private final Map<String, Throwable> failureDetails = new HashMap<>();
    private final Map<String, Integer> functionApplyCount = new LinkedHashMap<>();
    private final Map<String, Long> functionExecutionTime = new LinkedHashMap<>();
    private final AtomicInteger totalApplied = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger skipCount = new AtomicInteger(0);
    private final AtomicLong totalApplyTimeMillis = new AtomicLong(0);

    public void recordApply(String functionName, long durationMillis) {
        functionApplyCount.merge(functionName, 1, Integer::sum);
        functionExecutionTime.merge(functionName, durationMillis, Long::sum);
        totalApplyTimeMillis.addAndGet(durationMillis);
    }

    public void incrementSuccess() {
        successCount.incrementAndGet();
    }

    public void incrementSkipped() {
        skipCount.incrementAndGet();
    }

    public void incrementTotalApplied() {
        totalApplied.incrementAndGet();
    }

    public void incrementFailure(String stepName, Throwable ex) {
        failureCount.incrementAndGet();
        addFiledSteps(stepName);
        addFailureDetails(stepName, ex);
    }

    private void addFiledSteps(String stepName) {
        this.failedSteps.add(stepName);
    }

    private void addFailureDetails(String stepName, Throwable ex) {
        this.failureDetails.put(stepName, ex);
    }

    // Getter methods adapt to Atomic types
    private int getTotalApplied() {
        return totalApplied.get();
    }

    private int getSuccessCount() {
        return successCount.get();
    }

    private int getFailureCount() {
        return failureCount.get();
    }

    private long getTotalApplyTimeMillis() {
        return totalApplyTimeMillis.get();
    }

    public void printApplyStats() {
        if (getTotalApplied() == 0) {
            logger.info("📭 No apply steps were registered or executed.");
            return;
        }

        //@formatter:off

        String perStepStats = functionApplyCount.entrySet().stream().map(e -> {
            String step = e.getKey();
            int count = e.getValue();
            long time = functionExecutionTime.getOrDefault(step, 0L);
            return String.format("    🔹 %s -> %d times, %d ms", step, count, time);
        }).reduce("", "%s\n%s"::formatted);


        // 💥 Failed steps
        String failedSummary = failedSteps.isEmpty()
                ? "🎉 No failed steps!"
                : "\uD83D\uDCA5 Failed Steps: %s".formatted(String.join(", ", failedSteps));

        // 📊 Final formatted block (same as filterStats style)
        String formatted = String.format("""
                📊 Apply Function Stats:
                  ✅ Successful:        %d
                  ❌ Failed:            %d
                  🗿Total skipped:     %d
                  📦 Total Applied:     %d
                  🔢 Total Steps:       %d
                  ⏱️  Total Time:        %d ms
                  🔁 Per-Function Stats:
                        %s
                        %s
            """,
                successCount.get(),
                failureCount.get(),
                skipCount.get(),
                totalApplied.get(),
                functionApplyCount.size(),
                totalApplyTimeMillis.get(),
                perStepStats,
                failedSummary
        );
        //@formatter:on

        logger.info(">\n{}\n<", formatted);
    }

    public void printFailedDetails() {
        // Group by (stepName + message) and count
        //@formatter:off
        Map<String, Long> grouped = getFailureDetails()
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(e -> "%s | %s".formatted(e.getKey(), e.getValue().getMessage()),
                        LinkedHashMap::new,
                        Collectors.counting()));
        //@formatter:on


        if (grouped.isEmpty()) {
            logger.info("✅ No failed steps to report.");
        } else {
            logger.warn("❌ Failure Summary:");
            grouped.forEach((key, count) -> {
                String[] parts = key.split(" \\| ", 2);
                String step = parts[0];
                String message = parts.length > 1 ? parts[1] : "unknown";
                logger.warn("  💥 Step '{}': \"{}\" occurred {} time(s)", step, message, count);
            });
        }
    }

}

