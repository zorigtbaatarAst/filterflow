package mn.astvision.filterflow.util.helpers;

import lombok.extern.slf4j.Slf4j;
import mn.astvision.filterflow.annotations.FilterIgnore;
import mn.astvision.filterflow.annotations.VirtualField;
import mn.astvision.filterflow.annotations.VirtualObject;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.util.ConversionUtil;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.query.Criteria;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.reflect.FieldUtils.getAllFields;

/**
 * @author zorigtbaatar
 */

@Slf4j
public class GlobalSearchResolver {
    private static final ConcurrentMap<Class<?>, List<String>> SEARCHABLE_STRING_FIELDS_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, List<String>> NUMERIC_FIELDS_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, List<String>> DATE_FIELDS_CACHE = new ConcurrentHashMap<>();

    public static <T> Criteria buildGenericSearch(String keyword, FilterOptions options, Class<T> targetType) {
        if (keyword == null || keyword.isBlank()) return new Criteria();

        int maxDepth = options.getGlobalSearchDepth();
        Set<String> allowedFields = options.getAllowedGlobalSearchFields();
        Set<String> excludedFields = options.getExcludedGlobalSearchFields();

        List<String> stringFields = getFilteredFields(SEARCHABLE_STRING_FIELDS_CACHE, targetType, type -> collectSearchableTextFields(type, maxDepth), maxDepth, allowedFields, excludedFields);
        List<String> numericFields = getFilteredFields(NUMERIC_FIELDS_CACHE, targetType, type -> collectNumericFields(type, maxDepth), maxDepth, allowedFields, excludedFields);
        List<String> dateFields = getFilteredFields(DATE_FIELDS_CACHE, targetType, type -> collectDateFields(type, maxDepth), maxDepth, allowedFields, excludedFields);

        List<Criteria> conditions = new ArrayList<>();
        conditions.addAll(buildStringConditions(keyword, stringFields));
        conditions.addAll(buildNumericConditions(keyword, numericFields));
        conditions.addAll(buildDateConditions(keyword, dateFields));

        if (options.isDebug()) {
            logGlobalSearchDebug(keyword, targetType, options, conditions);
        }

        return combineConditions(conditions);
    }


    private static List<Criteria> buildStringConditions(String keyword, List<String> fields) {
        if (fields.isEmpty()) return Collections.emptyList();
        Pattern pattern = Pattern.compile(keyword, Pattern.CASE_INSENSITIVE);
        List<Criteria> conditions = new ArrayList<>();
        for (String field : fields) {
            conditions.add(Criteria.where(field).regex(pattern));
        }
        return conditions;
    }

    private static List<Criteria> buildNumericConditions(String keyword, List<String> fields) {
        if (fields.isEmpty()) return Collections.emptyList();
        List<Criteria> conditions = new ArrayList<>();
        try {
            double value = Double.parseDouble(keyword);
            for (String field : fields) {
                conditions.add(Criteria.where(field).is(value));
            }
        } catch (NumberFormatException ignored) {
        }
        return conditions;
    }

    private static List<Criteria> buildDateConditions(String keyword, List<String> fields) {
        if (fields.isEmpty()) return Collections.emptyList();
        Date parsedDate = ConversionUtil.tryParseToDate(keyword);
        if (parsedDate == null) return Collections.emptyList();

        List<Criteria> conditions = new ArrayList<>();
        for (String field : fields) {
            conditions.add(Criteria.where(field).is(parsedDate));
        }
        return conditions;
    }


    private static List<String> getFilteredFields(ConcurrentMap<Class<?>, List<String>> cache, Class<?> type, java.util.function.Function<Class<?>, List<String>> collector, int maxDepth, Set<String> allowedFields, Set<String> excludedFields) {
        List<String> fields = cache.computeIfAbsent(type, collector);

        if ((allowedFields != null && !allowedFields.isEmpty()) || (excludedFields != null && !excludedFields.isEmpty())) {
            fields = fields.stream().filter(f -> (allowedFields == null || allowedFields.isEmpty() || allowedFields.contains(f)) && (excludedFields == null || !excludedFields.contains(f))).toList();
        }
        return fields;
    }

    /*
    maxDepth = 0 → search only top-level fields, skip everything inside collections/maps/objects.
    maxDepth = 1 → search top-level fields + direct elements of collections/maps, but not nested objects inside those elements.
    maxDepth = 2 → allow one extra level inside nested elements, and so on.
    * */
    private static List<String> collectFields(Class<?> type, String prefix, boolean ignoreSuperClass, FieldTypeChecker checker, int currentDepth, int maxDepth) {
        List<String> fields = new ArrayList<>();
        Field[] declaredFields = ignoreSuperClass ? type.getDeclaredFields() : getAllFields(type);

        if (currentDepth > maxDepth) return fields;

        for (Field field : declaredFields) {
            field.setAccessible(true);

            if (field.isAnnotationPresent(Transient.class)) continue;
            if (field.isAnnotationPresent(FilterIgnore.class)) continue;

            Class<?> fieldType = field.getType();

            // Accept primitive/wrapper/other types by checker
            if (checker.accept(fieldType)) {
                fields.add(prefix + field.getName());
                continue;
            }

            // Handle @VirtualField
            VirtualField vf = field.getAnnotation(VirtualField.class);
            if (vf != null && checker.accept(fieldType)) {
                fields.add(prefix + field.getName());
                continue;
            }

            // Recurse into @VirtualObject
            VirtualObject vo = field.getAnnotation(VirtualObject.class);
            if (vo != null && vo.projectFields().length > 0) {
                for (String proj : vo.projectFields()) {
                    fields.add(prefix + field.getName() + "." + proj);
                }
                continue;
            } else if (vo != null) {
                fields.addAll(collectFields(fieldType, prefix + field.getName() + ".", true, checker, currentDepth + 1, maxDepth));
                continue;
            }

            if (Collection.class.isAssignableFrom(fieldType)) {
                fields.addAll(handleCollectionField(field, prefix, checker, currentDepth, maxDepth));
            } else if (Map.class.isAssignableFrom(fieldType)) {
                fields.addAll(handleMapField(field, prefix, checker, currentDepth, maxDepth));
            }
        }

        return fields;
    }

    private static Criteria combineConditions(List<Criteria> conditions) {
        if (conditions.isEmpty()) return new Criteria();
        if (conditions.size() == 1) return conditions.getFirst();
        return new Criteria().orOperator(conditions.toArray(new Criteria[0]));
    }


    private static List<String> handleCollectionField(Field field, String prefix, FieldTypeChecker checker, int currentDepth, int maxDepth) {
        List<String> fields = new ArrayList<>();
        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType pType)) return fields;

        Type actualType = pType.getActualTypeArguments()[0];
        if (!(actualType instanceof Class<?> elementType)) return fields;

        if (checker.accept(elementType)) {
            fields.add(prefix + field.getName()); // List<String/Number/Date> case
        } else {
            String nestedPrefix = prefix + field.getName() + ".";
            fields.addAll(collectFields(elementType, nestedPrefix, true, checker, currentDepth + 1, maxDepth));
        }
        return fields;
    }

    //TODO: fix it
    private static List<String> handleMapField(Field field, String prefix, FieldTypeChecker checker, int currentDepth, int maxDepth) {
        List<String> fields = new ArrayList<>();
        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType pType)) return fields;

        Type valueType = pType.getActualTypeArguments()[1];
        if (valueType instanceof Class<?> valueClass) {
            if (checker.accept(valueClass)) {
                fields.add(prefix + field.getName());
            } else if (Collection.class.isAssignableFrom(valueClass)) {
                // Get the element type of the list
                Type innerType = ((ParameterizedType) pType.getActualTypeArguments()[1]).getActualTypeArguments()[0];
                if (innerType instanceof Class<?> elemClass) {
                    String nestedPrefix = prefix + field.getName() + ".*."; // use .* for dynamic map keys
                    fields.addAll(collectFields(elemClass, nestedPrefix, true, checker, currentDepth + 1, maxDepth));
                }
            } else {
                String nestedPrefix = prefix + field.getName() + ".*."; // dynamic map key
                fields.addAll(collectFields(valueClass, nestedPrefix, true, checker, currentDepth + 1, maxDepth));
            }
        }
        return fields;
    }


    public static List<String> collectSearchableTextFields(Class<?> type, int maxDepth) {
        return collectFields(type, "", true, t -> t.equals(String.class), 0, maxDepth);
    }

    public static List<String> collectNumericFields(Class<?> type, int maxDepth) {
        return collectFields(type, "", true, t -> Number.class.isAssignableFrom(t) || t.isPrimitive() && (t == int.class || t == long.class || t == double.class || t == float.class), 0, maxDepth

        );
    }

    public static List<String> collectDateFields(Class<?> type, int maxDepth) {
        return collectFields(type, "", true, t -> Date.class.isAssignableFrom(t) || Temporal.class.isAssignableFrom(t), 0, maxDepth);
    }

    private static <T> void logGlobalSearchDebug(String keyword, Class<T> targetType, FilterOptions options, List<Criteria> conditions) {

        int maxDepth = options.getGlobalSearchDepth();
        Set<String> allowedFields = options.getAllowedGlobalSearchFields();
        Set<String> excludedFields = options.getExcludedGlobalSearchFields();

        //@formatter:off
        List<String> allFields = Stream.of(
                        getFilteredFields(SEARCHABLE_STRING_FIELDS_CACHE, targetType,
                                type -> collectSearchableTextFields(type, maxDepth), maxDepth, allowedFields, excludedFields),
                        getFilteredFields(NUMERIC_FIELDS_CACHE, targetType,
                                type -> collectNumericFields(type, maxDepth), maxDepth, allowedFields, excludedFields),
                        getFilteredFields(DATE_FIELDS_CACHE, targetType,
                                type -> collectDateFields(type, maxDepth), maxDepth, allowedFields, excludedFields)
                )
                .flatMap(Collection::stream)
                .toList();

        Set<String> readableConditions = conditions.stream()
                .map(c -> {
                    Map<String, Object> map = c.getCriteriaObject();
                    return map.keySet().stream()
                            .map(k -> k + "=" + map.get(k))
                            .toList();
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new)); // remove duplicates while keeping order
        //@formatter:on


        log.info("Global search: keyword: {}, allowed: {}, excluded: {}, fields size: {}, conditions size: {}", keyword, allowedFields, excludedFields, allFields.size(), conditions.size());
        log.info("Global search details:\nallFields: {}\nconditions: {}", allFields, readableConditions);
    }
}
