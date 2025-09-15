package mn.astvision.filterflow.component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.astvision.filterflow.handlers.OperatorHandlerRegistry;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.model.enums.FilterOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author zorigtbaatar
 */

public class FilterComponentDeserializer extends JsonDeserializer<FilterComponent> {
    private static String findClosestMatch(String input, Set<String> options) {
        int minDistance = Integer.MAX_VALUE;
        String closest = null;

        for (String option : options) {
            int dist = levenshtein(input.toUpperCase(), option.toUpperCase());
            if (dist < minDistance) {
                minDistance = dist;
                closest = option;
            }
        }

        return minDistance <= 3 ? closest : null;
    }

    private static int levenshtein(String a, String b) {
        int[][] dp = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= b.length(); j++) dp[0][j] = j;

        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
            }
        }

        return dp[a.length()][b.length()];
    }

    private static String getGroupedOperatorsMessage() {
        return """
                🧮 Comparison:
                    - EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, BETWEEN, NOT_BETWEEN
                
                🔠 String Matching:
                    - CONTAINS_WORD, STARTS_WITH, ENDS_WITH, LIKE, REGEX
                
                📦 Collections:
                    - IN, NOT_IN
                
                🧾 Map-Specific:
                    - MAP_KEY_EQUALS, MAP_VALUE_EQUALS, MAP_VALUE_CONTAINS, MAP_VALUE_EXISTS
                
                🔍 Existence & Null:
                    - EXISTS, IS_NULL, IS_NOT_NULL
                
                ⚙️ Special:
                    - EXPR, GLOBAL
                """;
    }

    @Override
    public FilterComponent deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        String type = node.has("type") ? node.get("type").asText() : node.has("components") ? "group" : "filter";

        if (type.equalsIgnoreCase("group")) {
            // Default logicMode
            String logicStr = node.has("logic") ? node.get("logic").asText() :
                    node.has("logicMode") ? node.get("logicMode").asText() : "AND";
            FilterLogicMode logicMode = FilterLogicMode.valueOf(logicStr.toUpperCase());

            List<FilterComponent> components = new ArrayList<>();

            for (JsonNode comp : node.get("components")) {
                components.add(mapper.treeToValue(comp, FilterComponent.class));
            }

            FilterGroup group = new FilterGroup();
            group.setLogicMode(logicMode);
            group.getComponents().addAll(components);
            return group;

        } else if (type.equals("filter")) {
            // Parse logic
            String logicStr = node.has("logic") ? node.get("logic").asText() : "AND";
            FilterLogicMode logic = FilterLogicMode.valueOf(logicStr.toUpperCase());

            String rawOperator = node.get("operator").asText();
            Set<String> registeredOperators = OperatorHandlerRegistry.getRegisteredOperators();

            boolean convertible = FilterOperator.isConvertible(rawOperator.toUpperCase());
            boolean containsRegisteredOp = registeredOperators.contains(rawOperator.toUpperCase());

            if (!containsRegisteredOp && !convertible) {
                String suggestion = findClosestMatch(rawOperator, registeredOperators);
                StringBuilder msg = new StringBuilder();
                msg.append("Invalid operator '").append(rawOperator).append("'.\n");
                if (suggestion != null) {
                    msg.append("Did you mean '").append(suggestion).append("'?\n");
                }
                msg.append("Allowed operators:\n").append(OperatorHandlerRegistry.getGroupedOperatorsMessage());
                throw new JsonParseException(p, msg.toString());
            }

            String field = null;
            if (!rawOperator.equalsIgnoreCase("GLOBAL") && !rawOperator.equalsIgnoreCase("EXPR")) {
                JsonNode fieldNode = node.get("field");
                if (fieldNode == null || fieldNode.isNull()) {
                    throw new JsonParseException(p, "Field is required for operator: %s".formatted(rawOperator));
                }
                field = fieldNode.asText();
            }

            JsonNode valueNode = node.get("value");
            Object value = mapper.treeToValue(valueNode, Object.class);

            if (convertible)
                rawOperator = FilterOperator.fromString(rawOperator).name();

            return new FilterRequest(logic, field, rawOperator, value);
        }

        throw new JsonParseException(p, "Unable to determine FilterComponent type (missing 'components', 'operator', or explicit 'type')");
    }

}
