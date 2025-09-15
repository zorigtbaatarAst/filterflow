package mn.astvision.filterflow;

import mn.astvision.filterflow.builders.base.DefaultFilterFlowBuilder;
import mn.astvision.filterflow.component.FilterContextHolder;
import mn.astvision.filterflow.component.factory.FilterExecutorFactory;

/// @author zorigtbaatar

/**
 * A static utility to access {@link FilterExecutorFactory} safely from anywhere,
 * without direct dependency injection.
 */
public class FilterFlow {
    private FilterFlow() {
        // Prevent instantiation
    }

    /**
     * Starts a builder chain for executing a filtered query.
     *
     * @param clazz Mongo entity class
     * @param <T>   entity type
     * @return fluent builder
     */
    public static <T> DefaultFilterFlowBuilder<T> ofType(Class<T> clazz) {
        try {
            FilterExecutorFactory factory = FilterContextHolder.getBean(FilterExecutorFactory.class);
            return new DefaultFilterFlowBuilder<>(factory.forType(clazz));
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Spring context is not initialized. Cannot access FilterExecutorFactory.", e);
        }
    }
}

