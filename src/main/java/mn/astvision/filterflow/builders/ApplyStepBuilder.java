package mn.astvision.filterflow.builders;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import mn.astvision.filterflow.component.FilterExecutionStatsHolder;
import mn.astvision.filterflow.component.executors.ApplyStepExecutor;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.ApplyStats;
import mn.astvision.filterflow.model.FilterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

public class ApplyStepBuilder<T> {
    private final ApplyStepExecutor<T> applyStepExecutor;
    private final FilterOptions options;
    private final Logger log = LoggerFactory.getLogger(ApplyStepBuilder.class);
    private BiConsumer<String, Throwable> errorHandler;

    public ApplyStepBuilder(FilterOptions options) {
        this.applyStepExecutor = new ApplyStepExecutor<>();
        this.options = options;
    }

    private void debug(String message, Object... args) {
        if (options.isDebug()) {
            log.info("[DEBUG] {} {}", message, args);
        }
    }

    public ApplyStepBuilder<T> apply(Consumer<T> consumer) {
        applyStepExecutor.addApplyStep(consumer);
        return this;
    }

    public ApplyStepBuilder<T> apply(Consumer<T> consumer, Consumer<Exception> onFailure) {
        applyStepExecutor.addApplyStep(item -> {
            try {
                consumer.accept(item);
            } catch (Exception e) {
                onFailure.accept(e);
            }
        });
        return this;
    }

    public ApplyStepBuilder<T> applyIf(boolean condition, Consumer<T> consumer) {
        if (condition) {
            applyStepExecutor.addApplyStep(consumer);
        }
        return this;
    }

    public ApplyStepBuilder<T> applyIf(Predicate<T> predicate, Consumer<T> consumer) {
        applyStepExecutor.addApplyStep(item -> {
            if (predicate.test(item)) {
                consumer.accept(item);
            }
        });
        return this;
    }

    public ApplyStepBuilder<T> applyWithTimeout(Consumer<T> consumer, long timeoutMillis) {
        applyStepExecutor.addApplyStep(item -> {
            try {
                CompletableFuture.runAsync(() -> consumer.accept(item)).get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new FilterException("Step timed out", e);
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return this;
    }

    public ApplyStepBuilder<T> logBefore(BiConsumer<String, T> logger) {
        applyStepExecutor.setPreLogHook(logger);
        return this;
    }

    public ApplyStepBuilder<T> logAfter(BiConsumer<String, T> logger) {
        applyStepExecutor.setPostLogHook(logger);
        return this;
    }

    public ApplyStepBuilder<T> validateWith(Validator validator) {
        return apply(item -> {
            Set<ConstraintViolation<T>> violations = validator.validate(item);
            if (!violations.isEmpty()) throw new ConstraintViolationException(violations);
        });
    }

    public ApplyStepBuilder<T> onError(BiConsumer<String, Throwable> handler) {
        this.errorHandler = handler;
        return this;
    }

    private void prepareApplyStepBuilderLogging() {
        if (options.isLogBeforeEnabled()) {
            this.logBefore((stage, item) -> {
                log.info("ApplyStep Before Stage: {}, Item: {}", stage, item);
            });
        }

        if (options.isLogAfterEnabled()) {
            this.logAfter((stage, item) -> {
                log.info("ApplyStep After Stage: {}, Item: {}", stage, item);
            });
        }

        if (options.isExecutionStatLoggerEnabled()) {
            FilterExecutionStatsHolder.printStatLog();
        }
    }

    public void execute(List<T> items) {
        prepareApplyStepBuilderLogging();
        debug("Applying " + items.size() + " items size", "Apply step count: ", applyStepExecutor.getApplySteps().size());

        //@formatter:off
        applyStepExecutor.withErrorHandler(errorHandler)
                .withOptions(options)
                .execute(items);
        //@formatter:on

        applyStepExecutor.clearApplySteps();
    }

    public void execute(Stream<T> items) {
        prepareApplyStepBuilderLogging();
        debug("Applying Stream items size", "Apply step count: ", applyStepExecutor.getApplySteps().size());

        //@formatter:off
        applyStepExecutor.withErrorHandler(errorHandler)
                .withOptions(options)
                .execute(items);
        //@formatter:on

        applyStepExecutor.clearApplySteps();
    }


    public ApplyStats getStats() {
        return applyStepExecutor.getApplyStats();
    }

    public ApplyStepBuilder<T> endApply() {
        return this;
    }
}
