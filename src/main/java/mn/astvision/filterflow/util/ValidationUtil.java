package mn.astvision.filterflow.util;

import mn.astvision.filterflow.annotations.FilterIgnore;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterOperator;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author zorigtbaatar
 */


public class ValidationUtil {
    private final static Logger log = LoggerFactory.getLogger(ValidationUtil.class);
    private static final Map<Class<?>, Map<String, Field>> FIELD_CACHE = new HashMap<>();

    public static void validateFieldAndOperation(FilterRequest filter, Class<?> targetType) {
        String[] fieldParts = filter.getField().split("\\.");
        Object value = filter.getValue();
        Class<?> currentType = targetType;

        for (int i = 0; i < fieldParts.length; i++) {
            String part = fieldParts[i].replaceAll("\\[\\d*]", "");
            Field field = findAndValidateField(part, currentType);

            currentType = resolveFieldType(field);

            if (currentType == Object.class) {
                log.warn("Field '{}' has raw generic type; skipping strict type validation", field.getName());
                continue;
            }

            if (isLastPart(i, fieldParts)) {
                validateFinalField(filter, field, currentType, value);
            }
        }
    }

    public static Class<?> resolveField(String fieldName, Class<?> rootType) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new FilterException("Field name cannot be null or empty");
        }

        try {
            String[] parts = fieldName.split("\\.");
            Class<?> currentType = rootType;
            Field field;

            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                field = findAndValidateField(part, currentType);

                boolean isLast = isLastPart(i, parts);
                currentType = resolveFieldType(field);

                if (isLast) {
                    return ConversionUtil.normalize(currentType);
                }
            }
        } catch (FilterException e) {
            throw e;
        } catch (Exception e) {
            log.error("unexpected error while resolving field '{}' for type '{}'", fieldName, rootType.getSimpleName(), e);
        }

        throw new FilterException("Could not resolve field type for '%s' from %s".formatted(fieldName, rootType.getSimpleName()));
    }

    private static Field findAndValidateField(String fieldName, Class<?> type) {
        Map<String, Field> fields = FIELD_CACHE.computeIfAbsent(type, k -> new ConcurrentHashMap<>());

        return fields.computeIfAbsent(fieldName, fn -> {
            Field f = ReflectionUtils.findField(type, fn);
            if (f == null) throw new FilterException("Invalid field: '%s' in %s".formatted(fn, type.getSimpleName()));
            if (f.isAnnotationPresent(Transient.class)) {
                List<String> fNames = collectTransientFieldNames(type);
                throw new FilterException("The field '%s' is marked as @Transient and cannot be used for filtering.".formatted(fn), type, " transient fields: " + fNames);
            }

            if (f.isAnnotationPresent(Deprecated.class))
                throw new FilterException("The field '%s' is marked as @Deprecated and is not supported for filtering.".formatted(fn));

            if (f.isAnnotationPresent(FilterIgnore.class)) {
                throw new FilterException("Field '%s' is marked with @FilterIgnore or @JsonIgnore and cannot be filtered.".formatted(fieldName));
            }

            f.setAccessible(true);
            return f;
        });
    }

    private static Class<?> resolveFieldType(Field field) {
        try {
            Class<?> rawType = field.getType();
            Type genericType = field.getGenericType();

            // Handle Collection types like List<Foo>
            if (Collection.class.isAssignableFrom(rawType)) {
                if (genericType instanceof ParameterizedType parameterizedType) {
                    Type actualType = parameterizedType.getActualTypeArguments()[0];
                    return extractClassFromType(actualType);
                }
                return Object.class; // raw List
            }

            // Handle Map types like Map<String, Foo>
            if (Map.class.isAssignableFrom(rawType)) {
                if (genericType instanceof ParameterizedType parameterizedType) {
                    Type valueType = parameterizedType.getActualTypeArguments()[1]; // V in Map<K, V>
                    return extractClassFromType(valueType);
                }
                return Object.class; // raw Map
            }

            // Default non-generic type
            return rawType;

        } catch (Exception e) {
            throw new FilterException("Failed to resolve type of field '%s': %s".formatted(field.getName(), e.getMessage()), e);
        }
    }

    /**
     * Helper to extract a Class<?> from a Type including support for wildcards and parameterized types.
     */
    private static Class<?> extractClassFromType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }

        if (type instanceof ParameterizedType pt) {
            Type raw = pt.getRawType();
            if (raw instanceof Class<?> rawClass) {
                return rawClass;
            }
        }

        if (type instanceof WildcardType wildcard) {
            Type[] upperBounds = wildcard.getUpperBounds();
            if (upperBounds.length > 0 && upperBounds[0] instanceof Class<?> upperClass) {
                return upperClass;
            }
        }

        return Object.class; // fallback if we can't resolve
    }

    private static boolean isLastPart(int index, String[] parts) {
        return index == parts.length - 1;
    }

    private static void validateFinalMapFiled(FilterRequest filter, String fieldName, Object value) {
        switch (filter.getOperator()) {
            case "MAP_VALUE_EQUALS", "MAP_VALUE_CONTAINS" -> {
                if (!(value instanceof Map<?, ?> mapVal)) {
                    throw new FilterException("%s operator expects a Map value for field '%s', but got: %s".formatted(filter.getOperator(), fieldName, value == null ? "null" : value.getClass().getSimpleName()));
                }
                if (mapVal.size() != 1) {
                    throw new FilterException("%s operator requires a single key-value pair Map for field '%s', but got size: %d".formatted(filter.getOperator(), fieldName, mapVal.size()));
                }
                Map.Entry<?, ?> entry = mapVal.entrySet().iterator().next();
                if (!(entry.getKey() instanceof String)) {
                    throw new FilterException("%s operator expects String key for field '%s', but got: %s".formatted(filter.getOperator(), fieldName, entry.getKey().getClass().getSimpleName()));
                }
                return;
            }
            case "MAP_VALUE_EXISTS" -> {
                if (!(value instanceof String || value instanceof Boolean)) {
                    throw new FilterException("%s operator expects a String key or Boolean for field '%s', but got: %s".formatted(filter.getOperator(), fieldName, value == null ? "null" : value.getClass().getSimpleName()));
                }
                return;
            }
            case "MAP_KEY_EQUALS" -> {
                if (!(value instanceof String)) {
                    throw new FilterException("%s operator expects a String key for field '%s', but got: %s".formatted(filter.getOperator(), fieldName, value == null ? "null" : value.getClass().getSimpleName()));
                }
                return;
            }
        }
    }

    private static void validateFinalField(FilterRequest filter, Field field, Class<?> fieldType, Object value) {

        boolean convertible = FilterOperator.isConvertible(filter.getOperator());
        if (!convertible) return;

        if (FilterOperator.EXISTS.equals(filter.getOperator())) return;

        // Use 'field' for more descriptive errors
        String fieldName = "%s.%s".formatted(field.getDeclaringClass().getSimpleName(), field.getName());

        OperationUtil.validate(filter, fieldType, fieldName);

        if (Map.class.isAssignableFrom(fieldType)) {
            validateFinalMapFiled(filter, fieldName, value);
            return;
        }


        switch (filter.getOperator()) {
            case "REGEX" -> validateRegexPattern(value, fieldName);
            case "BETWEEN", "NOT_BETWEEN" -> validateBetween(value, fieldType, fieldName);
            case "CONTAINS_WORD" -> validatePatternOperators(filter, value, fieldName);
            case "IN", "NOT_IN" -> validateInOperators(value, fieldType, filter.getOperator(), fieldName);
            case "EXPR" -> validateExpr(value, fieldName);
            case "GLOBAL" ->
                    throw new FilterException("GLOBAL operator validation should be handled separately if needed");
            default -> validateDefault(value, fieldType, fieldName);
        }
    }

    public static void validateExpression(Object expr) {
        if (expr instanceof Document document) {
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (!key.startsWith("$")) {
                    throw new FilterException("Invalid operator '%s'. Expression keys must start with '$'.".formatted(key));
                }

                validateExpression(value);
            }
        } else if (expr instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new FilterException("All map keys in expression must be strings. Found: %s".formatted(entry.getKey()));
                }
                validateExpression(entry.getValue());
            }
        } else if (expr instanceof List<?> list) {
            for (Object item : list) {
                validateExpression(item);
            }
        }
        // Primitives (String, Number, Boolean, etc.) are valid as-is
    }

    public static void validateAggregationOperations(List<AggregationOperation> operations) {
        if (operations == null) {
            throw new FilterException("Aggregation operations cannot be null");
        }

        try {
            Aggregation aggregation = Aggregation.newAggregation(operations);
            List<Document> pipeline = aggregation.toPipeline(Aggregation.DEFAULT_CONTEXT);

            for (int i = 0; i < pipeline.size(); i++) {
                Document stage = pipeline.get(i);

                if (stage == null || stage.isEmpty()) {
                    throw new FilterException("Aggregation stage at index %d is null or empty".formatted(i));
                }

                String stageName = stage.keySet().stream().findFirst().orElse("unknown");
                if (!stageName.startsWith("$")) {
                    throw new FilterException("Invalid stage name at index %d: must start with '$', but got '%s'".formatted(i, stageName));
                }
            }

        } catch (Exception e) {
            throw new FilterException("Invalid aggregation pipeline: %s".formatted(e.getMessage()), e);
        }
    }

    private static void validateRegexPattern(Object value, String fieldName) {
        if (!(value instanceof String pattern)) {
            throw new FilterException("REGEX operator expects String for '%s'".formatted(fieldName));
        }
        try {
            Pattern.compile(pattern);
        } catch (PatternSyntaxException ex) {
            throw new FilterException("Invalid REGEX pattern: %s".formatted(ex.getDescription()));
        }
    }

    private static void validatePatternOperators(FilterRequest filter, Object value, String fieldName) {
        if (!(value instanceof String s)) {
            throw new FilterException("%s expects String for '%s'".formatted(filter.getOperator(), fieldName));
        }
        String patternStr = "\\b%s\\b".formatted(Pattern.quote(s));

        try {
            Pattern.compile(patternStr);
        } catch (PatternSyntaxException ex) {
            throw new FilterException("Invalid pattern syntax: %s".formatted(ex.getDescription()));
        }
    }

    private static void validateBetween(Object value, Class<?> fieldType, String fieldName) {
        List<?> range = asList(value);
        if (range.size() != 2) {
            throw new FilterException("BETWEEN operator requires 2 values for '%s'".formatted(fieldName));
        }
        for (Object item : range) {
            if (ConversionUtil.tryParseTemporal(item, fieldType) != null) continue;
            if (!ConversionUtil.isCompatibleType(item, fieldType)) {
                throw new FilterException("Type mismatch in BETWEEN for '%s'".formatted(fieldName));
            }
            if (!(item instanceof Comparable<?>)) {
                throw new FilterException("Value '%s' in BETWEEN for '%s' is not Comparable".formatted(item, fieldName));
            }

            if (!Comparable.class.isAssignableFrom(fieldType)) {
                throw new FilterException("Field '%s' type '%s' is not Comparable".formatted(fieldName, fieldType.getSimpleName()));
            }
        }
    }

    private static void validateInOperators(Object value, Class<?> fieldType, String operator, String fieldName) {
        List<?> list = asList(value);
        if (list.isEmpty()) {
            throw new FilterException("%s requires non-empty list for '%s'".formatted(operator, fieldName));
        }
        for (Object item : list) {
            if (ConversionUtil.tryParseTemporal(item, fieldType) != null) continue;
            if (!ConversionUtil.isCompatibleType(item, fieldType)) {
                throw new FilterException("Type mismatch in %s for '%s'".formatted(operator, fieldName));
            }
        }
    }

    private static void validateNgram(Object value, String fieldName) {
        if (!(value instanceof String s) || s.length() < 2) {
            throw new FilterException("NGRAM requires string length >= 2 for '%s'".formatted(fieldName));
        }
    }

    private static void validateExpr(Object value, String fieldName) {
        if (!(value instanceof Map)) {
            throw new FilterException("EXPR requires Map/Document for '%s'".formatted(fieldName));
        }
    }

    private static void validateDefault(Object value, Class<?> fieldType, String fieldName) {
        if (ConversionUtil.tryParseTemporal(value, fieldType) != null) return;
        if (!ConversionUtil.isCompatibleType(value, fieldType)) {
            throw new FilterException("Type mismatch on field '%s'. Expected: %s, but got: %s. value -> %s".formatted(fieldName, fieldType.getSimpleName(), value == null ? "null" : value.getClass().getSimpleName(), value));
        }
    }

    private static List<?> asList(Object value) {
        if (value instanceof List<?>) {
            return (List<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return Collections.singletonList(value);
        }
    }

    private static List<String> collectTransientFieldNames(Class<?> type) {
        List<String> transientFields = new ArrayList<>();
        Class<?> current = type;

        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (field.isAnnotationPresent(Transient.class)) {
                    transientFields.add(field.getName());
                }
            }
            current = current.getSuperclass();
        }

        return transientFields;
    }


}