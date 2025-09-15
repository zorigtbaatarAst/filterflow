package mn.astvision.filterflow.model;

import lombok.Data;
import lombok.experimental.FieldNameConstants;
import mn.astvision.filterflow.component.FilterComponent;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.enums.FilterOperator;
import mn.astvision.filterflow.util.ConversionUtil;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zorigtbaatar
 */

@Data
@FieldNameConstants
public class FilterOptions {
    private boolean debug;
    private boolean resolveVF;
    private boolean skipCount;

    // report
    private DbExplainOptions dbExplainOptions = DbExplainOptions.byDefault();
    private boolean dbExplainPlanCacheStats; // plan cache stats
    private boolean dbExplainPlanCacheHistograms; // plan cache histograms

    private boolean executionStatLoggerEnabled;
    private float memoryThreshholdPercent;

    //projection
    private List<String> project;
    private List<String> exclude;

    //global search
    private Set<String> allowedGlobalSearchFields;
    private Set<String> excludedGlobalSearchFields;
    private int globalSearchDepth = 4;

    // apply
    private boolean parallel;
    private boolean failFast;
    private boolean logBeforeEnabled;
    private boolean logAfterEnabled;
    private boolean applyStatsEnabled;
    private boolean disableApplySteps;
    private List<Integer> skipStep;

    public static FilterOptions defaults() {
        return new FilterOptions();
    }

    public static Set<String> getAllowedFieldNames() {
        //@formatter:off
        Field[] declaredFields = FilterOptions.class.getDeclaredFields();
        return Arrays.stream(declaredFields)
                .map(Field::getName)
                .collect(Collectors.toSet());
        //@formatter:on
    }

    /**
     * Factory method to build FilterOptions from a FilterGroup by extracting
     * control flags and removing them from the filter group components.
     */
    public static FilterOptions fromFilterGroup(FilterGroup filters) {
        FilterOptions options = FilterOptions.defaults();
        Set<String> allowedKeys = getAllowedFieldNames();

        Map<String, Object> controlOptions = extractControlOptions(filters, allowedKeys);
        applyControlOptions(options, controlOptions);

        return options;
    }

    private static boolean pruneEmptyGroups(FilterGroup group, Set<String> allowedKeys, Map<String, Object> controlOptions) {
        if (group == null || group.getComponents() == null) return false;

        Iterator<FilterComponent> iterator = group.getComponents().iterator();

        while (iterator.hasNext()) {
            FilterComponent component = iterator.next();

            if (component instanceof FilterRequest req) {
                if (FilterOperator.CONTROL.equals(req.getOperator())) {
                    String key = req.getField();
                    if (key.contains(".")) {
                        // Nested field: allow if top-level exists in allowedKeys
                        String topLevelKey = key.split("\\.")[0];
                        if (allowedKeys.contains(topLevelKey)) {
                            controlOptions.put(key, req.getValue());
                            iterator.remove();
                        } else {
                            throw new FilterException(
                                    "Unknown control key: %s".formatted(key),
                                    FilterOptions.class,
                                    "allowed control keys: %s".formatted(getAllowedFieldNames())
                            );
                        }
                    } else if (allowedKeys.contains(key)) {
                        controlOptions.put(key, req.getValue());
                        iterator.remove();
                    } else {
                        throw new FilterException(
                                "Unknown control key: %s".formatted(key),
                                FilterOptions.class,
                                "allowed control keys: %s".formatted(getAllowedFieldNames())
                        );
                    }
                }
            } else if (component instanceof FilterGroup childGroup) {
                boolean hasContent = pruneEmptyGroups(childGroup, allowedKeys, controlOptions);
                if (!hasContent) {
                    iterator.remove();
                }
            }
        }

        return !group.getComponents().isEmpty();
    }


    private static Map<String, Object> extractControlOptions(FilterGroup filters, Set<String> allowedKeys) {
        Map<String, Object> controlOptions = new HashMap<>();
        pruneEmptyGroups(filters, allowedKeys, controlOptions);
        return controlOptions;
    }

    private static void applyControlOptions(Object target, Map<String, Object> controlOptions) {
        for (Map.Entry<String, Object> entry : controlOptions.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            try {
                if (key.contains(".")) {
                    // Nested property: split by dot
                    String[] parts = key.split("\\.", 2);
                    String parentFieldName = parts[0];
                    String childKey = parts[1];

                    Field parentField = target.getClass().getDeclaredField(parentFieldName);
                    parentField.setAccessible(true);
                    Object parentValue = parentField.get(target);

                    if (parentValue == null) {
                        // instantiate nested object if null
                        parentValue = parentField.getType().getDeclaredConstructor().newInstance();
                        parentField.set(target, parentValue);
                    }

                    // Recurse into nested object
                    Map<String, Object> childMap = Map.of(childKey, value);
                    applyControlOptions(parentValue, childMap);

                } else {
                    Field field = target.getClass().getDeclaredField(key);
                    field.setAccessible(true);
                    Object convertedValue = ConversionUtil.convertToExpectedType(value, field.getType());
                    field.set(target, convertedValue);
                }
            } catch (NoSuchFieldException | IllegalAccessException | InstantiationException |
                     java.lang.reflect.InvocationTargetException e) {
                System.err.printf("Warning: Cannot set field '%s': %s%n", key, e.getMessage());
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void processControlFlag(FilterRequest req, Iterator<FilterComponent> iterator, Map<String, Object> controlOptions, Set<String> allowedControlKeys) {
        if (FilterOperator.CONTROL.equals(req.getOperator())) {
            String key = req.getField();
            if (allowedControlKeys.contains(key)) {
                controlOptions.put(key, req.getValue());
                iterator.remove();
            } else {
                throw new FilterException("Unknown control key: %s".formatted(key), FilterOptions.class, "allowed control keys: %s".formatted(getAllowedFieldNames()));
            }
        }
    }

    public void extractFromFilterGroup(FilterGroup filters) {
        Set<String> allowedKeys = getAllowedFieldNames();
        Map<String, Object> controlOptions = extractControlOptions(filters, allowedKeys);
        applyControlOptions(this, controlOptions);
    }

    public void enableParallel() {
        this.parallel = true;
    }

    public void enableStats() {
        this.executionStatLoggerEnabled = true;
        this.applyStatsEnabled = true;
    }

    public void enableFailFast() {
        this.failFast = true;
    }


}
