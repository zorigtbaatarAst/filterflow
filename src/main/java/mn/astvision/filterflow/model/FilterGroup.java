package mn.astvision.filterflow.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import mn.astvision.filterflow.component.FilterComponent;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.model.enums.FilterOperator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zorigtbaatar
 */

@Getter
@Setter
@NoArgsConstructor
public class FilterGroup extends FilterComponent {
    private FilterLogicMode logic = FilterLogicMode.AND;
    private List<FilterComponent> components = new ArrayList<>();

    public FilterGroup(FilterLogicMode logic, FilterComponent... components) {
        this.logic = logic;
        this.components.addAll(Arrays.asList(components));
        this.normalize();
    }

    public static FilterGroup createFromFR(List<FilterRequest> requests) {
        FilterGroup filterGroup = new FilterGroup();
        filterGroup.addComponent(requests);
        return filterGroup;
    }

    public void clear() {
        this.components.clear();
    }

    public void normalize() {
        List<FilterComponent> normalized = new ArrayList<>();
        List<FilterComponent> buffer = new ArrayList<>();

        for (FilterComponent component : components) {
            if (component instanceof FilterRequest) {
                buffer.add(component);
            } else {
                if (!buffer.isEmpty()) {
                    FilterGroup subGroup = new FilterGroup();
                    subGroup.setLogicMode(FilterLogicMode.AND);
                    subGroup.setComponents(buffer);
                    normalized.add(subGroup);
                    buffer = new ArrayList<>();
                }
                normalized.add(component);
            }
        }

        // flush remaining
        if (!buffer.isEmpty()) {
            FilterGroup subGroup = new FilterGroup();
            subGroup.setLogicMode(FilterLogicMode.AND);
            subGroup.setComponents(buffer);
            normalized.add(subGroup);
        }

        components = normalized;
    }

    public void setLogicMode(FilterLogicMode logic) {
        this.logic = logic;
    }


    public void addComponent(FilterRequest filter) {
        components.add(filter);
        this.normalize();
    }

    public void addComponent(List<FilterRequest> filter) {
        components.addAll(filter);
        this.normalize();
    }


    public void addComponent(FilterComponent comp) {
        if (comp == null)
            return;

        if (comp == this) {
            throw new FilterException("Cannot add FilterGroup to itself");
        }

        if (comp instanceof FilterGroup group && group.containsComponent(this)) {
            throw new FilterException("Cannot add a parent FilterGroup to a child FilterGroup");
        }

        components.add(comp);
        this.normalize();
    }

    private boolean containsComponent(FilterComponent target) {
        for (FilterComponent c : components) {
            if (c == target) {
                return true;
            }
            if (c instanceof FilterGroup nested && nested.containsComponent(target)) {
                return true;
            }
        }
        return false;
    }


    public String toSimpleString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOGIC OPERATORS:\n");
        sb.append("  &&  : Logical AND — all conditions must be true\n");
        sb.append("  ||  : Logical OR  — at least one condition must be true\n\n");
        toSimpleString(sb, 0, true);
        return sb.toString();
    }

    private void toSimpleString(StringBuilder sb, int level, boolean isTopLevel) {
        String indent = "  ".repeat(level);

        if (!isTopLevel) {
            sb.append(indent).append("{ * Level ").append(level).append("\n");
        }

        for (FilterComponent component : components) {
            if (component instanceof FilterRequest cond) {
                String logic = cond.getLogic() == FilterLogicMode.OR ? "||" : "&&";
                //@formatter:off
                sb.append(indent).append(logic).append(" (")
                        .append(cond.getField())
                        .append(" ")
                        .append(FilterOperator.valueOf(cond.getOperator())
                                .getLogicExpression())
                        .append(" ")
                        .append(formatValue(cond.getValue()))
                        .append(")\n");
                //@formatter:on

            } else if (component instanceof FilterGroup nestedGroup) {
                sb.append(indent).append(nestedGroup.getLogic() == FilterLogicMode.OR ? "|| " : "&& ").append("Group\n");
                nestedGroup.toSimpleString(sb, level + 1, false);
                sb.append("\n");
            }
        }

        if (!isTopLevel) {
            sb.append(indent).append("}");
        }
    }

    public String toSLE() {
        Map<FilterRequest, String> symbolMap = new LinkedHashMap<>();
        StringBuilder legend = new StringBuilder("Legend:\n");
        int[] symbolIndex = {0};

        assignSymbols(this, symbolMap, legend, symbolIndex);
        return buildSymbolicExpression(this, symbolMap);
    }

    public String toSymbolicLogicExpression() {

        if (this.getComponents() == null || this.getComponents().isEmpty()) {
            return """
                    🧠 Filter Logic Structure Overview
                    ────────────────────────────────
                    🔹 Total Logic Components: 0
                    📏 Max Depth: 0
                    
                    Legend:
                    (no components)
                    
                    Logic Expression:
                    (empty)
                    """;
        }

        Map<FilterRequest, String> symbolMap = new LinkedHashMap<>();
        StringBuilder legend = new StringBuilder("Legend:\n");
        int[] symbolIndex = {0};

        assignSymbols(this, symbolMap, legend, symbolIndex);
        String expression = buildSymbolicExpression(this, symbolMap);

        Map<FilterLogicMode, Integer> counts = countLogicOperations();
        StringBuilder logicCountsStr = new StringBuilder();
        for (FilterLogicMode mode : FilterLogicMode.values()) {
            int count = counts.getOrDefault(mode, 0);
            if (count > 0) {
                logicCountsStr.append("🤁 ").append(mode.name()).append(": ").append(count).append("\n");
            }
        }

        return String.format("""
                🧠 Filter Logic Structure Overview
                ────────────────────────────────
                🔹 Total Logic Components: %d
                %s
                📏 Max Depth: %d
                
                %s
                Logic Expression:
                %s
                """, countComponents(this), logicCountsStr, computeDepth(this), legend, expression);
    }

    String formatValue(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(Object::toString).collect(Collectors.joining(", ", "[", "]"));
        }

        if (value instanceof String str) {
            return "\"" + str + "\"";
        }

        return value.toString();
    }

    private void assignSymbols(FilterGroup group, Map<FilterRequest, String> map, StringBuilder legend, int[] symbolIndex) {
        for (FilterComponent component : group.getComponents()) {
            if (component instanceof FilterRequest cond) {
                String symbol = String.valueOf((char) ('A' + symbolIndex[0]++));
                map.put(cond, symbol);
                //@formatter:off
                legend.append(symbol).append(" = (")
                        .append(cond.getField())
                        .append(" ")
                        .append(FilterOperator.valueOf(cond.getOperator()).getLogicExpression())
                        .append(" ")
                        .append(cond.getValue())
                        .append(")\n");
                //@formatter:on
            } else if (component instanceof FilterGroup groupCond) {
                assignSymbols(groupCond, map, legend, symbolIndex);
            }
        }
    }

    private String buildSymbolicExpression(FilterGroup group, Map<FilterRequest, String> map) {
        if (group == null || group.getComponents().isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();

        boolean first = true;

        for (FilterComponent component : group.getComponents()) {
            String operator = switch (component.getLogic()) {
                case OR -> " || ";
                case AND -> " && ";
                case NOT -> " ! "; // handle if needed
                case NOR -> " NOR "; // handle if needed
                default -> " && "; // fallback
            };

            String part;

            if (component instanceof FilterRequest cond) {
                part = map.get(cond);
            } else if (component instanceof FilterGroup innerGroup) {
                String nested = buildSymbolicExpression(innerGroup, map);

                if (innerGroup.getComponents().size() > 1) {
                    part = "(%s)".formatted(nested);
                } else {
                    part = nested;
                }
            } else {
                continue;
            }

            if (first) {
                builder.append(part);
                first = false;
            } else {
                builder.append(operator).append(part);
            }
        }

        return builder.toString();
    }

    public Map<FilterLogicMode, Integer> countLogicOperations() {
        Map<FilterLogicMode, Integer> counts = new EnumMap<>(FilterLogicMode.class);
        for (FilterLogicMode mode : FilterLogicMode.values()) {
            counts.put(mode, 0);
        }
        countLogicOperationsRecursive(this, counts);
        return counts;
    }

    private void countLogicOperationsRecursive(FilterGroup group, Map<FilterLogicMode, Integer> counts) {
        if (group.getComponents().size() > 1) {
            // A group with multiple components implies a logical operation
            // Use the logic of the first component to represent this group operation
            FilterLogicMode logicMode = group.getComponents().get(1).getLogic(); // e.g. B && C means B has logic AND
            counts.put(logicMode, counts.get(logicMode) + 1);
        }

        for (FilterComponent component : group.getComponents()) {
            if (component instanceof FilterGroup nestedGroup) {
                countLogicOperationsRecursive(nestedGroup, counts);
            }
        }
    }

    public int count() {
        return countComponents(this);
    }

    private int countComponents(FilterGroup group) {
        int count = 0;

        for (FilterComponent component : group.getComponents()) {
            if (component instanceof FilterRequest) {
                count += 1;
            } else if (component instanceof FilterGroup nested) {
                count += countComponents(nested); // recursively count inside
            }
        }

        return count;
    }

    public Map<FilterLogicMode, Integer> countLogicByMode() {
        // Use EnumMap for efficiency with enums
        Map<FilterLogicMode, Integer> logicCounts = new EnumMap<>(FilterLogicMode.class);

        // Initialize counts to zero for all logic modes
        for (FilterLogicMode mode : FilterLogicMode.values()) {
            logicCounts.put(mode, 0);
        }

        // Recursively collect logic counts
        collectLogicCounts(this, logicCounts);

        return logicCounts;
    }

    private void collectLogicCounts(FilterGroup group, Map<FilterLogicMode, Integer> logicCounts) {
        for (FilterComponent component : group.getComponents()) {
            // Count this component's logic mode
            FilterLogicMode logic = component.getLogic();
            logicCounts.put(logic, logicCounts.get(logic) + 1);

            // If the component is a nested group, recurse into it
            if (component instanceof FilterGroup nestedGroup) {
                collectLogicCounts(nestedGroup, logicCounts);
            }
        }
    }

    private int computeDepth(FilterGroup group) {
        if (group.getComponents().isEmpty()) {
            return 1;
        }

        //@formatter:off
        int maxNested = group.getComponents().stream()
                .filter(c -> c instanceof FilterGroup)
                .map(c -> ((FilterGroup) c))
                .mapToInt(this::computeDepth)
                .max()
                .orElse(0);
        //@formatter:on

        return 1 + maxNested;
    }


    @Override
    public FilterLogicMode getLogic() {
        return logic;
    }
}
