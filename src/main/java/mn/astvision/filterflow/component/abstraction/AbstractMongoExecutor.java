package mn.astvision.filterflow.component.abstraction;

import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.handlers.DbExplainHandler;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.util.CriteriaBuilderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author zorigtbaatar
 */

public abstract class AbstractMongoExecutor<T> {
    protected final MongoTemplate mongoTemplate;
    protected final Class<T> targetType;
    protected final FilterOptions options;
    protected final DbExplainHandler dbExplainHandler;
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected AbstractMongoExecutor(MongoTemplate mongoTemplate, FilterOptions options, Class<T> targetType) {
        if (mongoTemplate == null) throw new FilterException("MongoTemplate must not be null");

        this.mongoTemplate = Objects.requireNonNull(mongoTemplate);
        this.targetType = Objects.requireNonNull(targetType);
        this.options = options != null ? options : FilterOptions.defaults();
        this.dbExplainHandler = new DbExplainHandler(mongoTemplate, targetType, this.options.getDbExplainOptions());
    }

    protected Criteria buildCriteria(FilterGroup filterGroup) {
        return (filterGroup != null && !filterGroup.getComponents().isEmpty())
                ? CriteriaBuilderUtil.buildGroupCriteria(filterGroup, options, targetType)
                : new Criteria();
    }

    protected PersistentEntity<?, ?> getPersistentEntityOrThrow() {
        PersistentEntity<?, ?> entity = mongoTemplate.getConverter().getMappingContext().getPersistentEntity(targetType);
        if (entity == null) {
            throw new FilterException("Invalid entity: %s".formatted(targetType.getSimpleName()), targetType,
                    "Ensure the entity is registered in MongoTemplate's MappingContext");
        }
        return entity;
    }

    protected Set<String> getAllMappedFieldNames(PersistentEntity<?, ?> entity) {
        Set<String> fields = new TreeSet<>();
        collectFields(entity, "", fields);
        return fields;
    }

    private void collectFields(PersistentEntity<?, ?> entity, String prefix, Set<String> fields) {
        entity.doWithProperties((SimplePropertyHandler) property -> {
            String fieldName = prefix.isEmpty() ? property.getName() : prefix + "." + property.getName();
            fields.add(fieldName);
            Class<?> type = property.getType();
            if (!isSimpleType(type)) {
                PersistentEntity<?, ?> nested = mongoTemplate.getConverter().getMappingContext().getPersistentEntity(type);
                if (nested != null) collectFields(nested, fieldName, fields);
            }
        });
    }

    protected boolean isSimpleType(Class<?> type) {
        return org.springframework.data.mapping.model.SimpleTypeHolder.DEFAULT.isSimpleType(type)
                || type.getPackageName().startsWith("java.");
    }

    private void logTimeIfNeeded(String method, Instant start) {
        if (options.isExecutionStatLoggerEnabled()) {
            log.info("[LOG EXEC TIME] AggregationExecutor.{} executed in {} ms", method, Duration.between(start, Instant.now()).toMillis());
        }
    }

    protected void debug(String msg, Object... args) {
        if (!options.isDebug()) return;
        if (args.length > 0) log.info("[DEBUG] {} - {}", msg, Arrays.toString(args));
        else log.info("[DEBUG] {}", msg);
    }

    protected void warn(String msg, Object... args) {
        if (!options.isDebug()) return;
        if (args.length > 0) log.info("[WARN] {} - {}", msg, Arrays.toString(args));
        else log.info("[WARN] {}", msg);
    }

}
