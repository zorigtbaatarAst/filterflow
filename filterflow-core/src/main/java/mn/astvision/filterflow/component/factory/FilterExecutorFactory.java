package mn.astvision.filterflow.component.factory;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import mn.astvision.filterflow.component.executors.AggregationExecutor;
import mn.astvision.filterflow.component.executors.FilterExecutor;
import mn.astvision.filterflow.component.executors.SummaryExecutor;
import mn.astvision.filterflow.handlers.DefaultOperatorHandlers;
import mn.astvision.filterflow.handlers.OperatorHandlerRegistrar;
import mn.astvision.filterflow.model.FilterOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author zorigtbaatar
 */

@Component
@RequiredArgsConstructor
public class FilterExecutorFactory {
    private final MongoTemplate mongoTemplate;
    private final List<OperatorHandlerRegistrar> registrars;

    @PostConstruct
    public void registerOperationHandlers() {
        DefaultOperatorHandlers.registerAll();
        if (registrars == null) return;
        registrars.forEach(OperatorHandlerRegistrar::register);
    }

    public <T> FilterExecutor.Builder<T> forType(Class<T> clazz) {
        validateMongoEntity(clazz);
        return FilterExecutor.forType(clazz).withMongoTemplate(mongoTemplate);
    }

    public <T> SummaryExecutor<T> forSummaryType(Class<T> clazz, FilterOptions options) {
        validateMongoEntity(clazz);
        return SummaryExecutor.create(mongoTemplate, clazz, options);
    }

    public <T> AggregationExecutor.Builder<T> forAggregationType(Class<T> clazz) {
        validateMongoEntity(clazz);
        return AggregationExecutor.forType(clazz).withMongoTemplate(mongoTemplate);
    }

    private void validateMongoEntity(Class<?> clazz) {
        boolean exits = mongoTemplate.getConverter().getMappingContext().hasPersistentEntityFor(clazz);
        if (!exits) {
            throw new IllegalArgumentException("❌ Type " + clazz.getSimpleName() + " is not a valid MongoDB entity.");
        }
    }
}
