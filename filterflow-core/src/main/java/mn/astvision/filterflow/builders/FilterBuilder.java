package mn.astvision.filterflow.builders;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterOperator;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.data.annotation.Transient;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zorigtbaatar
 */

@Slf4j
@Getter
public class FilterBuilder<T> {
    private final Class<T> type;
    private final Map<String, FilterRequest> filterMap;

    private FilterBuilder(Class<T> type) {
        this.type = type;
        this.filterMap = new LinkedHashMap<>();
    }

    public static <T> FilterBuilder<T> ofType(Class<T> type) {
        return new FilterBuilder<>(type);
    }


    public FilterBuilder<T> fromObject(T instance, Set<String> include) {
        if (instance == null) return this;

        // Track visited objects to avoid circular references
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        collectFilters(null, instance, type, include, visited);

        return this;
    }

    private void collectFilters(String prefix, Object instance, Class<?> currentType, Set<String> include, Set<Object> visited) {
        if (instance == null || visited.contains(instance)) return;
        visited.add(instance);

        ReflectionUtils.doWithFields(currentType, field -> {
            field.setAccessible(true);

            if (field.isAnnotationPresent(Transient.class)) return;
            if (include != null && include.contains(field.getName())) return;

            Object value = field.get(instance);
            if (shouldSkipValue(value)) return;

            String fieldName = (prefix != null ? prefix + "." : "") + field.getName();

            // Handle collections or arrays
            if (value.getClass().isArray()) {
                addArrayFilters(fieldName, value);
                return;
            }

            if (value instanceof Collection<?> collection) {
                addCollectionFilters(fieldName, collection);
                return;
            }

            // Primitive or simple types: add filter directly
            if (isSimpleType(value.getClass())) {
                filterMap.put(fieldName, new FilterRequest(fieldName, FilterOperator.EQUALS, value));
            } else {
                // Nested object: recurse
                collectFilters(fieldName, value, field.getType(), include, visited);
            }
        });
    }

    private boolean isSimpleType(Class<?> clazz) {
        return clazz.isPrimitive() || clazz.equals(String.class) || Number.class.isAssignableFrom(clazz) || Boolean.class.isAssignableFrom(clazz) || Date.class.isAssignableFrom(clazz) || Enum.class.isAssignableFrom(clazz);
    }

    private void addArrayFilters(String fieldName, Object array) {
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            Object element = Array.get(array, i);
            if (element != null) {
                filterMap.put(fieldName + "[" + i + "]", new FilterRequest(fieldName, FilterOperator.IN, element));
            }
        }
    }

    private void addCollectionFilters(String fieldName, Collection<?> collection) {
        int index = 0;
        for (Object element : collection) {
            if (element != null) {
                filterMap.put(fieldName + "[" + index++ + "]", new FilterRequest(fieldName, FilterOperator.IN, element));
            }
        }
    }

    // Skip null, zero, false, empty container values
    private boolean shouldSkipValue(Object value) {
        return value == null || isZeroNumber(value) || isFalseBoolean(value) || isEmptyContainer(value);
    }

    public FilterBuilder<T> deleted(boolean value) {
        return eq("deleted", value);
    }

    public FilterBuilder<T> fromObject(T instance) {
        return fromObject(instance, null);
    }

    private boolean isFalseBoolean(Object value) {
        return value instanceof Boolean && !(Boolean) value;
    }

    private boolean isEmptyContainer(Object value) {
        if (value.getClass().isArray()) {
            return Array.getLength(value) == 0;
        }
        if (value instanceof Collection<?> collection) {
            return collection.isEmpty();
        }
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        return false;
    }

    private boolean isZeroNumber(Object value) {
        return switch (value) {
            case Byte b when b == 0 -> true;
            case Short s when s == 0 -> true;
            case Integer i when i == 0 -> true;
            case Long l when l == 0L -> true;
            case Float f when f == 0.0f -> true;
            case Double d when d == 0.0d -> true;
            default -> false;
        };
    }

    private FilterBuilder<T> addFilterInternal(String field, FilterOperator operator, Object value) {
        if (operator.equals(FilterOperator.GLOBAL) || operator.equals(FilterOperator.EXPR)) {
            if (value == null || (value instanceof String str && str.isEmpty())) {
                return this;
            }

            filterMap.put(operator.name(), new FilterRequest(operator.name(), operator, value));
            return this;
        }

        if (field == null) {
            log.debug("[FilterBuilder] Skipping filter: field is null, operator={}, value={}", operator, value);
            return this;
        }

        if (value == null && (operator != FilterOperator.IS_NULL && operator != FilterOperator.IS_NOT_NULL)) {
            log.debug("[FilterBuilder] Skipping filter: value is null, operator={}, field={}", operator, field);
            return this;
        }
        boolean exits = filterMap.containsKey(field);
        FilterRequest val = new FilterRequest(field, operator, value);

        if (exits) {
            FilterRequest filterRequest = filterMap.get(field);
            log.info("filter exits with key: {} and value: {} replacing ...", field, filterRequest.getValue());
        }

        filterMap.put(field, val);
        return this;
    }

    public FilterBuilder<T> asList() {
        return this;
    }

    public FilterBuilder<T> eq(String field, Object value) {
        if (ObjectUtils.isEmpty(value)) {
            return this;
        }

        return addFilterInternal(field, FilterOperator.EQUALS, value);
    }

    public FilterBuilder<T> ne(String field, Object value) {
        return addFilterInternal(field, FilterOperator.NOT_EQUALS, value);
    }

    public FilterBuilder<T> gt(String field, Object value) {
        return addFilterInternal(field, FilterOperator.GREATER_THAN, value);
    }

    public FilterBuilder<T> gte(String field, Object value) {
        return addFilterInternal(field, FilterOperator.GREATER_THAN_EQUAL, value);
    }

    public FilterBuilder<T> control(String field, Object value) {
        return addFilterInternal(field, FilterOperator.CONTROL, value);
    }

    public FilterBuilder<T> control(String field) {
        return addFilterInternal(field, FilterOperator.CONTROL, true);
    }


    public FilterBuilder<T> lt(String field, Object value) {
        return addFilterInternal(field, FilterOperator.LESS_THAN, value);
    }

    public FilterBuilder<T> lte(String field, Object value) {
        return addFilterInternal(field, FilterOperator.LESS_THAN_EQUAL, value);
    }

    public FilterBuilder<T> between(String field, Object start, Object end) {
        return addFilterInternal(field, FilterOperator.BETWEEN, List.of(start, end));
    }


    public FilterBuilder<T> startsWith(String field, String value) {
        return addFilterInternal(field, FilterOperator.STARTS_WITH, value);
    }

    public FilterBuilder<T> endsWith(String field, String value) {
        return addFilterInternal(field, FilterOperator.ENDS_WITH, value);
    }

    public FilterBuilder<T> like(String field, String value) {
        return addFilterInternal(field, FilterOperator.LIKE, value);
    }

    public FilterBuilder<T> global(String value) {
        return addFilterInternal(null, FilterOperator.GLOBAL, value);
    }

    public FilterBuilder<T> searchDepth(int maxDepth) {
        return addFilterInternal(FilterOptions.Fields.globalSearchDepth, FilterOperator.CONTROL, maxDepth);
    }

    public FilterBuilder<T> searchWithAllowed(String value, String... allowedFields) {
        if (allowedFields != null && allowedFields.length > 0) {
            Set<String> validAllowed = Arrays.stream(allowedFields)
                    .filter(this::isFieldValid)
                    .collect(Collectors.toCollection(LinkedHashSet::new)); // preserves order, removes duplicates

            if (!validAllowed.isEmpty()) {
                addFilterInternal(FilterOptions.Fields.allowedGlobalSearchFields, FilterOperator.CONTROL, validAllowed);
            }
        }

        return addFilterInternal(null, FilterOperator.GLOBAL, value);
    }

    public FilterBuilder<T> searchWithExcluded(String value, String... excludedFields) {
        if (excludedFields != null && excludedFields.length > 0) {
            Set<String> validExcluded = Arrays.stream(excludedFields)
                    .filter(this::isFieldValid)
                    .collect(Collectors.toCollection(LinkedHashSet::new)); // preserves order, removes duplicates

            if (!validExcluded.isEmpty()) {
                addFilterInternal(FilterOptions.Fields.excludedGlobalSearchFields, FilterOperator.CONTROL, validExcluded);
            }
        }

        return addFilterInternal(null, FilterOperator.GLOBAL, value);
    }

    private boolean isFieldValid(String fieldName) {
        try {
            Field field = FieldUtils.getField(type, fieldName, true);
            return field != null;
        } catch (Exception e) {
            log.warn("Field {} is not valid for global search. Error: {}", fieldName, e.getMessage());
            return false;
        }
    }

    public FilterBuilder<T> search(String value) {
        return global(value);
    }

    public FilterBuilder<T> in(String field, List<?> values) {
        return addFilterInternal(field, FilterOperator.IN, values);
    }

    public FilterBuilder<T> notIn(String field, List<?> values) {
        return addFilterInternal(field, FilterOperator.NOT_IN, values);
    }

    public FilterBuilder<T> exists(String field) {
        return addFilterInternal(field, FilterOperator.EXISTS, true);
    }

    public FilterBuilder<T> isNull(String field) {
        return addFilterInternal(field, FilterOperator.IS_NULL, null);
    }

    public FilterBuilder<T> isNotNull(String field) {
        return addFilterInternal(field, FilterOperator.IS_NOT_NULL, null);
    }

    public FilterBuilder<T> add(FilterRequest fr) {
        if (fr == null) return null;
        filterMap.put(fr.getField(), fr);

        return this;
    }

    public FilterBuilder<T> addList(List<FilterRequest> list) {
        if (list == null || list.isEmpty()) return this;

        for (FilterRequest fr : list) {
            if (fr == null) continue;

            if (fr.getOperator() != null && FilterOperator.GLOBAL.name().equals(fr.getOperator())) {
                filterMap.put(fr.getOperator(), fr);
                continue;
            }

            if (fr.getField() != null) {
                filterMap.put(fr.getField(), fr);
            }
        }
        return this;
    }

    public List<FilterRequest> build() {
        return new ArrayList<>(filterMap.values());
    }

}
