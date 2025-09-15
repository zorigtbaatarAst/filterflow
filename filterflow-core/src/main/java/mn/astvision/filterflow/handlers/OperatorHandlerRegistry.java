package mn.astvision.filterflow.handlers;



import mn.astvision.filterflow.model.enums.FilterOperator;

import java.util.*;

/**
 * @author zorigtbaatar
 */

public class OperatorHandlerRegistry {
    private static final Map<String, OperatorHandler> HANDLERS = new HashMap<>();
    private static final Map<String, String> CATEGORIES = new HashMap<>();

    public static void register(String operator, OperatorHandler handler) {
        if (HANDLERS.containsKey(operator)) {
            throw new IllegalStateException("Handler already registered for: %s".formatted(operator));
        }

        HANDLERS.put(operator, handler);
    }

    public static void register(String category, FilterOperator operator, OperatorHandler handler) {
        register(operator.name(), handler);
        CATEGORIES.put(operator.name(), category);
    }

    public static void register(String category, String operator, OperatorHandler handler) {
        register(operator, handler);
        CATEGORIES.put(operator, category);
    }


    public static OperatorHandler get(String operator) {
        return HANDLERS.get(operator);
    }

    public static boolean contains(String operator) {
        return HANDLERS.containsKey(operator);
    }

    public static Set<String> getRegisteredOperators() {
        return Collections.unmodifiableSet(HANDLERS.keySet());
    }

    public static String getCategory(String operator) {
        return CATEGORIES.get(operator);
    }

    public static Map<String, String> getAllCategories() {
        return Collections.unmodifiableMap(CATEGORIES);
    }

    public static String getGroupedOperatorsMessage() {
        Map<String, List<String>> grouped = new TreeMap<>();

        for (String op : getRegisteredOperators()) {
            String category = getCategory(op);
            if (category == null) category = "Other";
            grouped.computeIfAbsent(category, k -> new ArrayList<>()).add(op);
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> e : grouped.entrySet()) {
            sb.append("  ").append(e.getKey()).append(":\n    - ");
            sb.append(String.join(", ", e.getValue()));
            sb.append("\n\n");
        }
        return sb.toString();
    }

}
