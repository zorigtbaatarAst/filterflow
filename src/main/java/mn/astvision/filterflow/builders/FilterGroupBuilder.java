package mn.astvision.filterflow.builders;


import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.util.ExpressionParserUtil;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

/**
 * @author zorigtbaatar
 */

public class FilterGroupBuilder<T> {
    private final Deque<FilterGroup> groupStack = new ArrayDeque<>();

    private FilterGroupBuilder() {
        FilterGroup root = new FilterGroup();
        root.setLogicMode(FilterLogicMode.AND); // default root logic
        groupStack.push(root);
    }

    public static <T> FilterGroupBuilder<T> create() {
        return new FilterGroupBuilder<>();
    }


    // Add raw expression string: parses and builds nested groups accordingly
    public FilterGroupBuilder<T> expr(String expression) {
        FilterGroup current = groupStack.peek();
        if (current == null) throw new IllegalStateException("No active group");

        String normalized = normalizeExpression(expression);
        FilterGroup parsedGroup = ExpressionParserUtil.parseToFilterGroup(normalized);
        // Wrap parsed components in an AND group before adding to current
        FilterGroup wrapper = new FilterGroup();
        wrapper.setLogicMode(FilterLogicMode.AND);
        wrapper.getComponents().addAll(parsedGroup.getComponents());

        current.getComponents().add(new FilterGroup(FilterLogicMode.AND, wrapper));
        return this;
    }

    private String normalizeExpression(String expression) {
        if (expression == null) return null;

        // Trim spaces
        String trimmed = expression.trim();

        // Remove leading/trailing redundant quotes (single or double)
        while (
                (trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
                        (trimmed.startsWith("'") && trimmed.endsWith("'"))
        ) {
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
        }

        return trimmed;
    }

    public FilterGroupBuilder<T> filter(FilterRequest filterRequest) {
        FilterGroup current = groupStack.peek();
        if (current == null) throw new IllegalStateException("No active group");
        current.getComponents().add(filterRequest);
        return this;
    }

    public FilterGroupBuilder<T> filters(List<FilterRequest> filterRequests) {
        FilterGroup current = groupStack.peek();
        if (current == null) throw new IllegalStateException("No active group");
        current.getComponents().addAll(filterRequests);
        return this;
    }

    /**
     * Wraps a list of FilterRequests into a new group with AND logic by default, and adds that group.
     */
    public FilterGroupBuilder<T> groupFilters(List<FilterRequest> filterRequests) {
        return groupFilters(filterRequests, FilterLogicMode.AND);
    }

    /**
     * Wraps a list of FilterRequests into a new group with specified logic and adds that group to current.
     */
    public FilterGroupBuilder<T> groupFilters(List<FilterRequest> filterRequests, FilterLogicMode logicMode) {
        FilterGroup current = groupStack.peek();
        if (current == null) throw new IllegalStateException("No active group");

        FilterGroup subGroup = new FilterGroup();
        subGroup.setLogicMode(logicMode);

        for (FilterRequest request : filterRequests) {
            subGroup.getComponents().add(request);
        }

        current.getComponents().add(new FilterGroup(logicMode, subGroup));
        return this;
    }

    // Start a nested AND group
    public FilterGroupBuilder<T> andGroup(Function<FilterGroupBuilder<T>, FilterGroupBuilder<T>> nested) {
        return nest(FilterLogicMode.AND, nested);
    }

    // Start a nested OR group
    public FilterGroupBuilder<T> orGroup(Function<FilterGroupBuilder<T>, FilterGroupBuilder<T>> nested) {
        return nest(FilterLogicMode.OR, nested);
    }

    // Generic nesting helper
    private FilterGroupBuilder<T> nest(FilterLogicMode logic, Function<FilterGroupBuilder<T>, FilterGroupBuilder<T>> nested) {
        FilterGroup child = new FilterGroup();
        child.setLogicMode(logic);


        FilterGroup parent = groupStack.peek();
        if (parent == null) throw new IllegalStateException("No active group to nest into");
        parent.getComponents().add(new FilterGroup(logic, child));
        groupStack.push(child);
        nested.apply(this);
        groupStack.pop();
        return this;
    }

    // Generic nesting helper

    // --- Parsing logic ---

    public FilterGroup build() {
        if (groupStack.size() != 1) {
            throw new IllegalStateException("Unclosed nested groups exist");
        }
        return groupStack.peek();
    }
}
