package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.builders.ApplyStepBuilder;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface IApplyStepFlow<T, SELF> {
    ApplyStepBuilder<T> getApplyStepBuilder();

    default ApplyStepBuilder<T> applySteps() {
        return getApplyStepBuilder();
    }

    @SuppressWarnings("unchecked")
    default SELF self() {
        return (SELF) this;
    }

    default SELF logBefore(BiConsumer<String, T> logger) {
        getApplyStepBuilder().logBefore(logger).endApply();
        return self();
    }

    default SELF logAfter(BiConsumer<String, T> logger) {
        getApplyStepBuilder().logAfter(logger).endApply();
        return self();
    }

    default SELF apply(Consumer<T> consumer) {
        getApplyStepBuilder().apply(consumer).endApply();
        return self();
    }

    default SELF apply(Consumer<T> consumer, Consumer<Exception> onFailure) {
        getApplyStepBuilder().apply(consumer, onFailure).endApply();
        return self();
    }

    default SELF applyIf(boolean condition, Consumer<T> consumer) {
        getApplyStepBuilder().applyIf(condition, consumer).endApply();
        return self();
    }

    default SELF applyIf(Predicate<T> predicate, Consumer<T> consumer) {
        getApplyStepBuilder().applyIf(predicate, consumer).endApply();
        return self();
    }

    default SELF applyWithTimeout(Consumer<T> consumer, long timeoutMillis) {
        getApplyStepBuilder().applyWithTimeout(consumer, timeoutMillis).endApply();
        return self();
    }
}
