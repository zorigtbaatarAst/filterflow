package mn.astvision.filterflow.util;

import lombok.extern.slf4j.Slf4j;
import mn.astvision.filterflow.component.FilterComponent;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.model.enums.FilterOperator;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

@Slf4j
public class ExpressionParserUtil {
    //@formatter:off
static List<String> tokenDefs = List.of(
        "\\(",    // left parenthesis
        "\\)",    // right parenthesis
        "&&",     // logical AND
        "\\|\\|", // logical OR
        ">=",     // greater or equal
        "<=",     // less or equal
        "!=",     // not equal
        "==",     // equal
        ">",      // greater than
        "<",      // less than
        "=",      // simple equals (fallback)
        "\\[[^]]*\\]",        // array literal, e.g., [1,2,3]
        "\"[^\"]*\"|'[^']*'", // quoted strings (double or single)
        "[^\\s()&|<>=!]+"     // catch-all for other tokens
);
//@formatter:on

    static String tokenPattern = tokenDefs.stream().collect(Collectors.joining("|", "\\s*(", ")\\s*"));

    static Pattern TOKEN_PATTERN = Pattern.compile(tokenPattern);


    // Entry point to parse full expression string into FilterGroup
    public static FilterGroup parseToFilterGroup(String expression) {
        List<String> tokens = tokenize(expression);
        Parser parser = new Parser(tokens);
        return parser.parseExpression();
    }

    // Tokenizer splits by parentheses and operators, keeps conditions as tokens
    private static List<String> tokenize(String expression) {
        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(expression);
        while (matcher.find()) {
            tokens.add(matcher.group(1));
        }
        return tokens;
    }

    // Parse single filter condition like "field == value"
    public static FilterRequest parseSingleExpression(String expr) {
        expr = expr.trim();
        //@formatter:off
        List<FilterOperator> sortedOps = Stream.of(FilterOperator.values())
                .sorted((a, b) ->
                        Integer.compare(
                                b.getLogicExpression().length(),
                                a.getLogicExpression().length()
                        )).toList();
        //@formatter:on


        for (FilterOperator op : sortedOps) {
            String opExpr = op.getLogicExpression();

            // Use regex to split with operator surrounded by optional spaces
            String regex = "(?i)\\s*" + Pattern.quote(opExpr) + "\\s*";
            String[] split = expr.split(regex, 2);
            if (split.length == 2) {
                String field = split[0].trim();
                String value = split[1].trim();

                // Optionally remove quotes around value if present
                if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
                    value = value.substring(1, value.length() - 1);
                }

                return FilterRequest.createFilterRequest(field, op, value);
            }
        }
        throw new FilterException("No valid operator found in expression: " + expr);
    }

    public static String toReadableExpression(Criteria criteria) {
        Document criteriaObject = criteria.getCriteriaObject();
        return toReadableExpression(criteriaObject);
    }

    public static String toJson(Document doc) {
        if (doc == null) return null;
        try {
            Document converted = convertEnumValues(doc);
            return converted.toJson(JsonWriterSettings.builder().indent(true).build());
        } catch (Exception e) {
            log.error("❌ Failed to serialize Document to JSON: {}", e.getMessage(), e);
            return "{ \"error\": \"Failed to serialize filter document\" }";
        }
    }

    private static Document convertEnumValues(Document original) {
        Document copy = new Document();
        for (Map.Entry<String, Object> entry : original.entrySet()) {
            Object value = entry.getValue();
            switch (value) {
                case Enum<?> e -> copy.put(entry.getKey(), e.name()); // or e.toString()
                case Document doc -> copy.put(entry.getKey(), convertEnumValues(doc));
                case List<?> list -> copy.put(entry.getKey(), list.stream().map(v -> {
                    if (v instanceof Enum<?> e) return e.name();
                    if (v instanceof Document d) return convertEnumValues(d);
                    return v;
                }).toList());
                case null, default -> copy.put(entry.getKey(), value);
            }
        }
        return copy;
    }


    public static String toJson(Criteria criteria) {
        if (criteria == null) return null;

        Document criteriaObject = criteria.getCriteriaObject();
        return toJson(criteriaObject);
    }

    public static String toReadableExpression(Document doc) {
        if (doc.containsKey("$and")) {
            List<Document> andList = (List<Document>) doc.get("$and");
            return andList.stream()
                    .map(ExpressionParserUtil::toReadableExpression)
                    .collect(Collectors.joining(" && ", "(", ")"));
        } else if (doc.containsKey("$or")) {
            List<Document> orList = (List<Document>) doc.get("$or");
            return orList.stream()
                    .map(ExpressionParserUtil::toReadableExpression)
                    .collect(Collectors.joining(" || ", "(", ")"));
        } else {
            // No logical group → process all fields as implicit AND
            return doc.entrySet().stream().map(entry -> {
                String field = entry.getKey();
                Object val = entry.getValue();

                if (val instanceof Document valDoc) {
                    return valDoc.entrySet().stream().map(opEntry -> {
                        String op = opEntry.getKey();
                        Object v = opEntry.getValue();
                        String opStr = switch (op) {
                            case "$gt" -> ">";
                            case "$lt" -> "<";
                            case "$gte" -> ">=";
                            case "$lte" -> "<=";
                            case "$ne" -> "!=";
                            case "$eq" -> "==";
                            case "$in" -> "in";
                            default -> op;
                        };

                        if ("$in".equals(op)) {
                            return field + " in " + formatArray(v);
                        }

                        return field + " " + opStr + " " + formatValue(v);
                    }).collect(Collectors.joining(" && "));
                } else {
                    return field + " == " + formatValue(val);
                }
            }).collect(Collectors.joining(" && "));
        }
    }

    private static String formatArray(Object val) {
        if (val instanceof List<?> list) {
            // Join elements with commas, quote strings
            return "[" + list.stream().map(ExpressionParserUtil::formatValue).collect(Collectors.joining(", ")) + "]";
        }
        return val.toString();
    }

    private static String formatValue(Object val) {
        if (val instanceof String str) {
            return "\"" + str + "\"";
        }
        if (val instanceof Enum<?> e) {
            return "\"" + e.name() + "\"";
        }
        return val.toString();
    }

    static class Parser {
        private final List<String> tokens;
        private int pos = 0;

        public Parser(List<String> tokens) {
            this.tokens = tokens;
        }

        // Entry point: parse OR expressions (lowest precedence)
        public FilterGroup parseExpression() {
            FilterGroup left = parseAndExpression();

            while (match("||")) {
                FilterGroup right = parseAndExpression();

                if (allComponentsHaveLogic(left, FilterLogicMode.OR) && allComponentsHaveLogic(right, FilterLogicMode.OR)) {
                    left.getComponents().addAll(right.getComponents());
                } else {
                    FilterGroup orGroup = new FilterGroup();
                    orGroup.setLogicMode(FilterLogicMode.OR);
                    orGroup.getComponents().add(new FilterGroup(FilterLogicMode.OR, left));
                    orGroup.getComponents().add(new FilterGroup(FilterLogicMode.OR, right));
                    left = orGroup;
                }
            }

            return left;
        }

        // Parse AND expressions (higher precedence)
        public FilterGroup parseAndExpression() {
            FilterGroup left = parsePrimary();

            while (match("&&")) {
                FilterGroup right = parsePrimary();

                if (allComponentsHaveLogic(left, FilterLogicMode.AND) && allComponentsHaveLogic(right, FilterLogicMode.AND)) {
                    left.getComponents().addAll(right.getComponents());
                } else {
                    FilterGroup andGroup = new FilterGroup();
                    andGroup.setLogicMode(FilterLogicMode.AND);
                    FilterGroup g1 = new FilterGroup();
                    g1.addComponent(left);
                    FilterGroup g2 = new FilterGroup();
                    g2.addComponent(right);
                    andGroup.getComponents().add(g1);
                    andGroup.getComponents().add(g2);
                    left = andGroup;
                }
            }

            return left;
        }

        // Parse primary expressions: nested groups or filter conditions
        public FilterGroup parsePrimary() {
            if (match("(")) {
                FilterGroup group = parseExpression();
                expect(")");
                return group;
            }

            // parse a filter condition token triplet: field operator value
            String field = nextToken();
            if (field == null) throw new FilterException("Expected field");

            String operator = nextToken();
            if (operator == null) throw new FilterException("Expected operator after field: " + field);

            String valueToken = nextToken();
            if (valueToken == null) throw new FilterException("Expected value");

            StringBuilder valueBuilder = new StringBuilder(valueToken);
            int safetyCounter = 0;

            // If list, accumulate until closing ]
            if (valueToken.startsWith("[") && !valueToken.endsWith("]")) {
                while (!valueBuilder.toString().endsWith("]")) {
                    String next = nextToken();
                    if (next == null) {
                        throw new FilterException("Unclosed list value for operator " + operator);
                    }
                    valueBuilder.append(" ").append(next);
                    if (++safetyCounter > 100) {
                        throw new FilterException("Too many tokens without closing ] in list value");
                    }
                }
            }
            // If unquoted and has more tokens (e.g., date strings), consume until operator or end
            else if (!(valueToken.startsWith("\"") || valueToken.startsWith("'"))) {
                while (pos < tokens.size()) {
                    String next = tokens.get(pos);
                    // Stop if next is logical operator or closing group
                    if (List.of("&&", "||", ")").contains(next)) break;
                    valueBuilder.append(" ").append(nextToken());
                    if (++safetyCounter > 100) {
                        throw new FilterException("Too many tokens without logical break in value");
                    }
                }
            }

            String fullExpr = field + " " + operator + " " + valueBuilder.toString().trim();
            FilterRequest filter = ExpressionParserUtil.parseSingleExpression(fullExpr);

            FilterGroup group = new FilterGroup();
            group.setLogicMode(FilterLogicMode.AND);
            group.getComponents().add(filter);
            return group;
        }

        // Checks if all components inside group have the same logic mode
        private boolean allComponentsHaveLogic(FilterGroup group, FilterLogicMode logic) {
            if (group == null || group.getComponents().isEmpty()) return false;
            for (FilterComponent c : group.getComponents()) {
                if (c.getLogic() != logic) return false;
            }
            return true;
        }

        // Helpers to consume tokens
        private boolean match(String expected) {
            if (pos < tokens.size() && tokens.get(pos).equals(expected)) {
                pos++;
                return true;
            }
            return false;
        }

        private void expect(String expected) {
            if (!match(expected)) {
                throw new FilterException("Expected '" + expected + "' but found " + (pos < tokens.size() ? tokens.get(pos) : "end of expression"));
            }
        }

        private String nextToken() {
            if (pos >= tokens.size()) return null;
            return tokens.get(pos++);
        }
    }
}
