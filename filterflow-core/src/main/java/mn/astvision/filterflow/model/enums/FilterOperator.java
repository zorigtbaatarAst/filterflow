package mn.astvision.filterflow.model.enums;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;

/**
 * Enum representing various filter operators used in building query criteria.
 * Each operator has a corresponding symbolic or textual representation used in expressions.
 */
@Getter
public enum FilterOperator {
    /**
     * Equals operator (==).
     * Use case: field value equals the given value.
     * MongoDB: "$eq"
     */
    EQUALS("==", "eq"),

    /**
     * Not equals operator (!=).
     * Use case: field value is not equal to the given value.
     * MongoDB: "$ne"
     */
    NOT_EQUALS("!=", "ne"),

    /**
     * Greater than operator (>).
     * Use case: field value is strictly greater than the given value.
     * Typically used with numeric or date fields.
     * MongoDB: "$gt"
     */
    GREATER_THAN(">", "gt"),

    /**
     * Greater than or equal to operator (>=).
     * Use case: field value is greater than or equal to the given value.
     * MongoDB: "$gte"
     */
    GREATER_THAN_EQUAL(">=", "gte"),

    /**
     * Less than operator (<).
     * Use case: field value is strictly less than the given value.
     * MongoDB: "$lt"
     */
    LESS_THAN("<", "lt"),

    /**
     * Less than or equal to operator (<=).
     * Use case: field value is less than or equal to the given value.
     * MongoDB: "$lte"
     */
    LESS_THAN_EQUAL("<=", "lte"),

    /**
     * Contains word operator (~w).
     * Use case: matches whole words within the field's string value.
     * Useful for word-boundary sensitive searches.
     * MongoDB: regex with word boundaries, e.g., \bword\b
     */
    CONTAINS_WORD("~w", "cw"),


    /**
     * Starts with operator (^).
     * Use case: field value starts with the given substring (case-insensitive).
     * MongoDB: regex with pattern "^value"
     */
    STARTS_WITH("^", "sw"),

    /**
     * Ends with operator ($).
     * Use case: field value ends with the given substring (case-insensitive).
     * MongoDB: regex with pattern "value$"
     */
    ENDS_WITH("$", "eq"),

    /**
     * Like operator ("like").
     * Use case: case-insensitive regex matching for more flexible string patterns.
     * Useful when complex pattern matching is required.
     * Converts wildcards (*, ?) to regex internally.
     * MongoDB: general regex (user provides pattern)
     */
    LIKE("*", "like"),

    /**
     * In operator ("in").
     * Use case: checks if field value is contained in a list of values.
     * Useful for filtering by multiple possible values.
     * MongoDB: "$in"
     */
    IN("in", "in"),

    /**
     * Not in operator (!in).
     * Use case: checks if field value is NOT contained in a list of values.
     * MongoDB: "$nin"
     */
    NOT_IN("!in", "nin"),

    /**
     * Exists operator ("exists").
     * Use case: checks if a field exists in the document.
     * Useful for filtering documents that contain or lack a certain field.
     * MongoDB: "$exists"
     */
    EXISTS("exists", "exists"),

    /**
     * Expression operator ("expr").
     * Use case: allows raw MongoDB expressions or custom criteria.
     * Useful for advanced queries not expressible by standard operators.
     * MongoDB: "$expr"
     */
    EXPR("expr", "expr"),

    /**
     * Is null operator ("null").
     * Use case: filters documents where the field value is null.
     * MongoDB: Uses equality comparison (`field: null`).
     * Note: MongoDB considers missing fields as null in this context.
     */
    IS_NULL("null", "null"),

    /**
     * Is not null operator ("!null").
     * Use case: filters documents where the field value is not null.
     * MongoDB: Uses inequality comparison (`field: {$ne: null}`).
     */
    IS_NOT_NULL("!null", "notNull"),

    /**
     * Regex operator ("r").
     * Use case: filters documents where the field matches a regular expression.
     * Offers fine-grained and flexible text matching.
     * MongoDB: Uses regex.
     */
    REGEX("r", "regex"),

    /**
     * Between operator ("between").
     * Use case: filters documents where the field value falls within a specified range (inclusive).
     * Typically requires exactly two values representing the range bounds.
     * MongoDB: Use "$gte" and "$lte" together.
     */
    BETWEEN("between", "gte/lte"),
    NOT_BETWEEN("!between", "notGte/notLte"),

    /**
     * Global operator ("@").
     * Use case: applies a global search across multiple fields.
     * Generally used for full-text search or generic search queries.
     * MongoDB: Use "$text"
     */
    GLOBAL("@", "text"),

    MAP_VALUE_EQUALS(":=", "mve"),
    MAP_VALUE_CONTAINS(":~", "mvc"),
    MAP_VALUE_EXISTS(":?", "mve"),
    MAP_KEY_EQUALS("key=", "mke"),
    CONTROL("#", "ctl");


    final String logicExpression;
    private final String altName;

    FilterOperator(String logicExpression, String serializedName) {
        this.logicExpression = logicExpression;
        this.altName = serializedName;
    }


    /**
     * Resolves the FilterOperator enum from a given string representation.
     *
     * @param value the string representation of the operator (e.g. "==", "in", "~")
     * @return corresponding FilterOperator enum
     * @throws IllegalArgumentException if no matching operator is found
     */
    public static FilterOperator fromValue(String value) {
        for (FilterOperator op : values()) {
            if (op.logicExpression.equalsIgnoreCase(value)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown filter operator: " + value);
    }

    public static FilterOperator fromString(String value) {
        if (value == null) throw new IllegalArgumentException("FilterOperator cannot be null");

        String v = value.trim().toLowerCase();

        for (FilterOperator op : values()) {
            if (op.name().equalsIgnoreCase(v) ||
                    op.logicExpression.equalsIgnoreCase(v) ||
                    (op.altName != null && op.altName.equalsIgnoreCase(v))
            ) {
                return op;
            }
        }

        throw new IllegalArgumentException("Unknown FilterOperator: " + value);
    }

    public static boolean isConvertible(String val) {
        if (val == null) return false;
        String v = val.trim().toLowerCase();
        for (FilterOperator op : values()) {
            if (op.name().equalsIgnoreCase(v)
                    || op.logicExpression.equalsIgnoreCase(v)
                    || (op.altName != null && op.altName.equalsIgnoreCase(v))) {
                return true;
            }
        }
        return false;
    }

    public boolean convertable(String val) {
        if (val == null) return false;
        String v = val.trim().toLowerCase();
        return name().equalsIgnoreCase(v)
                || logicExpression.equalsIgnoreCase(v)
                || (altName != null && altName.equalsIgnoreCase(v));
    }

    public boolean equals(String val) {
        return convertable(val);
    }

    @JsonValue
    public String getLogicExpression() {
        return logicExpression;
    }

    @JsonValue
    public String getAltName() {
        return altName;
    }
}
