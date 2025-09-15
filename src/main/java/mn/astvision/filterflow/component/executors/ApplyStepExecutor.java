package mn.astvision.filterflow.component.executors;

import lombok.Data;
import mn.astvision.filterflow.model.ApplyStats;
import mn.astvision.filterflow.model.FilterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

@Data
public class ApplyStepExecutor<T> {
    private static final Logger logger = LoggerFactory.getLogger(ApplyStepExecutor.class);
    private final List<Consumer<T>> applySteps;
    private final ApplyStats applyStats;
    private BiConsumer<String, T> preLogHook;
    private BiConsumer<String, T> postLogHook;

    private FilterOptions options;
    private BiConsumer<String, Throwable> errorHandler;

    public ApplyStepExecutor() {
        this.applySteps = new ArrayList<>();
        this.applyStats = new ApplyStats();
    }

    private static <T> void safeCallHook(BiConsumer<String, T> hook, String stepName, T item) {
        if (hook == null) return;
        try {
            hook.accept(stepName, item);
        } catch (Exception ex) {
            logger.debug("Hook failed for '{}': {}", stepName, ex.toString());
        }
    }

    private static void safeCallErrorHandler(BiConsumer<String, Throwable> errorHandler, String stepName, Exception e) {
        if (errorHandler == null) return;
        try {
            errorHandler.accept(stepName, e);
        } catch (Exception handlerEx) {
            logger.error("safe call error handler failed for '{}': {}", stepName, handlerEx.toString());
        }
    }

    private static void recordSuccess(ApplyStats stats, String stepName, long startTime, FilterOptions options) {
        if (!options.isApplyStatsEnabled()) return;
        long duration = (System.nanoTime() - startTime) / 1_000_000;
        stats.incrementSuccess();
        stats.recordApply(stepName, duration);
    }

    private static void recordFailure(ApplyStats stats, String stepName, long startTime, Exception e, FilterOptions options) {
        logger.warn("⚠️ Apply step '{}' failed: {}", stepName, e.getMessage());

        if (!options.isApplyStatsEnabled()) return;
        long duration = (System.nanoTime() - startTime) / 1_000_000;
        stats.incrementFailure(stepName, e);
        stats.recordApply(stepName, duration);
    }

    static <T> ApplyStepExecutor<T> create() {
        return new ApplyStepExecutor<T>();
    }

    public ApplyStepExecutor<T> withErrorHandler(BiConsumer<String, Throwable> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    public ApplyStepExecutor<T> withOptions(FilterOptions options) {
        this.options = options;
        return this;
    }


    public void addApplyStep(Consumer<T> step) {
        this.applySteps.add(step);

    }

    public void addApplyStep() {

    }

    public void execute(List<T> items) {
        if (options.isDisableApplySteps()) return;

        //@formatter:off
        Stream<T> stream = options.isParallel()
                ? items.parallelStream()
                : items.stream();
        //@formatter:on

        stream.forEach(this::execute);

        if (options.isApplyStatsEnabled()) {
            applyStats.printApplyStats();
            applyStats.printFailedDetails();
        }
    }

    public void execute(Stream<T> stream) {
        if (options.isDisableApplySteps()) return;

        //@formatter:off
        Stream<T> effectiveStream = options.isParallel()
                ? stream.parallel()
                : stream.sequential();
        //@formatter:on

        effectiveStream.forEach(this::execute);

        if (options.isApplyStatsEnabled()) {
            applyStats.printApplyStats();
            applyStats.printFailedDetails();
        }
    }


    public void execute(T item) {
        if (options == null) this.options = FilterOptions.defaults();

        for (int i = 0; i < applySteps.size(); i++) {
            String stepName = "step#%d".formatted(i + 1);

            if (options.getSkipStep() != null && options.getSkipStep().contains(i + 1)) {
                applyStats.incrementSkipped();
                continue;
            }

            Consumer<T> step = applySteps.get(i);
            applyStats.incrementTotalApplied();

            safeCallHook(preLogHook, stepName, item);

            long startTime = options.isApplyStatsEnabled() ? System.nanoTime() : 0;

            try {
                step.accept(item);
                recordSuccess(applyStats, stepName, startTime, options);
            } catch (Exception e) {
                recordFailure(applyStats, stepName, startTime, e, options);
                safeCallErrorHandler(errorHandler, stepName, e);
                if (options.isFailFast()) throw e;
            }

            safeCallHook(postLogHook, stepName, item);
        }
    }

    public void clearApplySteps() {
        applySteps.clear();
    }
}
