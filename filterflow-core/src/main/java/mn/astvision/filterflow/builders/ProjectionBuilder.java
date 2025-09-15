package mn.astvision.filterflow.builders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

import java.beans.Introspector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

public class ProjectionBuilder<T> {
    private final static Logger log = LoggerFactory.getLogger(ProjectionBuilder.class);
    private static final int MAX_CACHE_SIZE = 500;
    private static final Map<String, ProjectionOperation> CACHE;

    static {
        LinkedHashMap<String, ProjectionOperation> cacheManager = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ProjectionOperation> eldest) {
                return size() > MAX_CACHE_SIZE;
            }
        };
        CACHE = Collections.synchronizedMap(cacheManager);
    }

    private final Class<T> targetType;
    private final Set<String> fields = new LinkedHashSet<>();
    private MongoTemplate mongoTemplate;

    private ProjectionBuilder(Class<T> targetType) {
        Assert.notNull(targetType, "Target type must not be null");
        this.targetType = targetType;
    }

    public static <T> ProjectionBuilder<T> ofType(Class<T> targetType) {
        return new ProjectionBuilder<T>(targetType);
    }

    public static void clearCache() {
        CACHE.clear();
    }

    public static int cacheSize() {
        return CACHE.size();
    }

    public ProjectionBuilder<T> withTemplate(MongoTemplate mongoTemplate) {
        Assert.notNull(mongoTemplate, "MongoTemplate must not be null");
        this.mongoTemplate = mongoTemplate;
        return this;
    }

    public ProjectionBuilder<T> merge(ProjectionBuilder<T> other) {
        this.fields.addAll(other.fields);
        return this;
    }

    public ProjectionBuilder<T> reset() {
        this.fields.clear();
        return this;
    }

    public void set(List<AggregationOperation> operations) {
        if (operations == null) throw new IllegalArgumentException("Operations cannot be null");
        int lastIndex = operations.size() - 1;

        ProjectionOperation newProj = build();

        if (lastIndex >= 0 && operations.get(lastIndex) instanceof ProjectionOperation) {
            operations.set(lastIndex, newProj);
        } else {
            operations.add(newProj);
        }
    }

    private boolean isValidField(String field) {
        return field != null && !field.trim().isEmpty() && !field.contains("$") && field.matches("^[a-zA-Z0-9_.]+$");
    }

    public ProjectionBuilder<T> withFieldIf(boolean condition, String fieldName) {
        if (condition && isValidField(fieldName)) {
            fields.add(fieldName);
        }
        return this;
    }

    public ProjectionBuilder<T> withFields(List<String> fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) return this;

        if (mongoTemplate != null) {
            PersistentEntity<?, ?> entity = getPersistentProperties();

            for (String field : fieldNames) {
                if (entity != null && entity.getPersistentProperty(field) == null) {
                    log.warn("Invalid field '{}' for projection on {}", field, targetType.getSimpleName());
                    continue;
                }
                fields.add(field);
            }
        } else {
            fieldNames.stream().filter(this::isValidField).forEach(fields::add);
        }
        return this;
    }

    private PersistentEntity<?, ?> getPersistentProperties() {
        //@formatter:off
        return mongoTemplate.getConverter()
                .getMappingContext()
                .getPersistentEntity(targetType);
        //@formatter:on

    }

    public ProjectionBuilder<T> withInclude(String... fieldNames) {
        if (fieldNames != null) {
            withFields(Arrays.asList(fieldNames));
        }
        return this;
    }

    public ProjectionBuilder<T> withExclude(String... fieldNames) {
        if (fieldNames != null) {
            Set<String> toRemove = new HashSet<>(Arrays.asList(fieldNames));
            fields.removeIf(toRemove::contains);
        }
        return this;
    }

    public ProjectionBuilder<T> copy() {
        ProjectionBuilder<T> copy = ProjectionBuilder.ofType(this.targetType);
        copy.fields.addAll(this.fields);
        copy.mongoTemplate = this.mongoTemplate;
        return copy;
    }

    public boolean hasProjections() {
        return !fields.isEmpty();
    }

    public ProjectionBuilder<T> withExcludeFields(List<String> excludeFields) {
        if (excludeFields == null) excludeFields = List.of();

        List<String> allFields;
        if (mongoTemplate != null) {
            PersistentEntity<?, ?> entity = getPersistentProperties();

            if (entity == null) throw new IllegalStateException("Cannot determine PersistentEntity for " + targetType);

            allFields = new ArrayList<>();
            entity.doWithProperties((SimplePropertyHandler) property -> allFields.add(property.getName()));
        } else {
            allFields = Stream.of(targetType.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());
        }

        final Set<String> exclusions = new HashSet<>(excludeFields);
        allFields.stream().filter(f -> !exclusions.contains(f)).forEach(fields::add);

        return this;
    }

    /**
     * Auto projection by mapping each Java field name using the provided mapper function to Mongo document field.
     *
     * @param fieldMapper Function to map Java property names to document field names (can be null for identity).
     * @return this builder
     */
    public ProjectionBuilder<T> withAutoProjection(Function<String, String> fieldMapper) {
        if (mongoTemplate != null) {
            PersistentEntity<?, ?> entity = getPersistentProperties();
            if (entity != null) {
                entity.doWithProperties((SimplePropertyHandler) property -> {
                    String javaField = property.getName();
                    String mappedField = (fieldMapper != null) ? fieldMapper.apply(javaField) : javaField;
                    fields.add(mappedField);
                });
            }
        } else {
            for (Field field : targetType.getDeclaredFields()) {
                String javaField = field.getName();
                String mappedField = (fieldMapper != null) ? fieldMapper.apply(javaField) : javaField;
                fields.add(mappedField);
            }
        }
        return this;
    }

    /**
     * Auto projection based on projectionType class or interface.
     * Supports interface getters or POJO fields.
     *
     * @param projectionType class or interface for projection
     * @return this builder
     */
    public <P> ProjectionBuilder<T> withAutoProjection(Class<P> projectionType) {
        if (mongoTemplate == null) throw new IllegalStateException("MongoTemplate is required for auto projection");

        MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext = mongoTemplate.getConverter().getMappingContext();

        PersistentEntity<?, ?> entity = mappingContext.getPersistentEntity(targetType);
        if (entity == null) throw new IllegalStateException("Cannot determine PersistentEntity for " + targetType);

        if (projectionType.isInterface()) {
            for (Method method : projectionType.getMethods()) {
                if (method.getDeclaringClass() == Object.class) continue;
                if (!method.getName().startsWith("get")) continue;

                String propertyName = Introspector.decapitalize(method.getName().substring(3));
                addProjectionField(entity, propertyName);
            }
        } else {
            for (Field field : projectionType.getDeclaredFields()) {
                addProjectionField(entity, field.getName());
            }
        }
        return this;
    }

    private void addProjectionField(PersistentEntity<?, ?> entity, String propertyName) {
        PersistentProperty<?> property = entity.getPersistentProperty(propertyName);
        if (property instanceof MongoPersistentProperty mongoProperty) {
            fields.add(mongoProperty.getFieldName());
        } else if (property != null) {
            fields.add(propertyName);
        }
    }

    public ProjectionOperation build() {
        if (fields.isEmpty()) {
            log.info("No projection fields for {}", targetType.getSimpleName());
            return Aggregation.project(); // empty projection
        }

        String cacheKey = buildCacheKey();

        return CACHE.computeIfAbsent(cacheKey, key -> {
            log.info("Building and caching ProjectionOperation for key={}", key);
            ProjectionOperation proj = Aggregation.project();
            for (String field : fields) {
                proj = proj.and(field).as(field);
            }
            return proj;
        });
    }

    private String buildCacheKey() {
        List<String> sortedFields = new ArrayList<>(fields);
        Collections.sort(sortedFields);
        return targetType.getName() + "::" + String.join(",", sortedFields);
    }
}
