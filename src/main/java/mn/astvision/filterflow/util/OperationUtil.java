package mn.astvision.filterflow.util;

import jakarta.validation.constraints.NotNull;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterOperator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;

import static mn.astvision.filterflow.model.enums.FilterOperator.*;


/**
 * @author zorigtbaatar
 */

public class OperationUtil {
    public static final Map<Class<?>, EnumSet<FilterOperator>> ALLOWED_OPERATORS = new HashMap<>();

    //@formatter:off
    static {
        // String fields
        ALLOWED_OPERATORS.put(String.class, EnumSet.of(
                        EQUALS, NOT_EQUALS,
                        STARTS_WITH, ENDS_WITH,
                        IN, NOT_IN,
                        IS_NULL, IS_NOT_NULL,
                        LIKE, REGEX,
                        CONTAINS_WORD
                )
        );

        // Number types
        EnumSet<FilterOperator> numberOps = EnumSet.of(
                EQUALS, NOT_EQUALS,
                LESS_THAN, LESS_THAN_EQUAL,
                GREATER_THAN, GREATER_THAN_EQUAL,
                IN, NOT_IN,
                IS_NULL, IS_NOT_NULL,
                BETWEEN,
                NOT_BETWEEN
        );

        ALLOWED_OPERATORS.put(Integer.class, numberOps);
        ALLOWED_OPERATORS.put(Long.class, numberOps);
        ALLOWED_OPERATORS.put(Double.class, numberOps);
        ALLOWED_OPERATORS.put(Float.class, numberOps);

        // Date/Time types
        EnumSet<FilterOperator> dateOps = EnumSet.of(
                EQUALS, NOT_EQUALS,
                LESS_THAN, LESS_THAN_EQUAL,
                GREATER_THAN, GREATER_THAN_EQUAL,
                IS_NULL, IS_NOT_NULL,
                BETWEEN,
                NOT_BETWEEN
        );

        ALLOWED_OPERATORS.put(LocalDateTime.class, dateOps);
        ALLOWED_OPERATORS.put(LocalDate.class, dateOps);
        ALLOWED_OPERATORS.put(LocalTime.class, dateOps);
        ALLOWED_OPERATORS.put(Date.class, dateOps);
        ALLOWED_OPERATORS.put(ZonedDateTime.class, dateOps);

        // Boolean
        ALLOWED_OPERATORS.put(Boolean.class, EnumSet.of(
                EQUALS, NOT_EQUALS,
                IS_NULL, IS_NOT_NULL
        ));

        // Enum (matched via .isAssignableFrom)
        ALLOWED_OPERATORS.put(Enum.class, EnumSet.of(
                EQUALS, NOT_EQUALS,
                IN, NOT_IN,
                IS_NULL, IS_NOT_NULL)
        );

        ALLOWED_OPERATORS.put(Map.class, EnumSet.of(
                EXPR,
                MAP_KEY_EQUALS,
                MAP_VALUE_EQUALS,
                MAP_VALUE_CONTAINS,
                MAP_VALUE_EXISTS
        ));

        ALLOWED_OPERATORS.put(Collection.class, EnumSet.of(
                IN, NOT_IN, IS_NULL, IS_NOT_NULL
        ));
    }
    //@formatter:on


    public static boolean isOperatorAllowed(Class<?> fieldType, String operator) {
        FilterOperator op = null;
        if (operator == null) return false;
        try {
            op = valueOf(operator);
        } catch (Exception e) {
            return false;
        }

        fieldType = ConversionUtil.normalize(fieldType);

        for (Map.Entry<Class<?>, EnumSet<FilterOperator>> entry : ALLOWED_OPERATORS.entrySet()) {
            if (entry.getKey().isAssignableFrom(fieldType)) {
                return entry.getValue().contains(op);
            }
        }

        return false;
    }

    public static void validate(FilterRequest filter, Class<?> fieldType, String fieldName) {
        boolean operatorAllowed = isOperatorAllowed(fieldType, filter.getOperator());
        if (!operatorAllowed) {
            throw new FilterException("Operator '%s' is not allowed for field '%s'. Allowed: %s".formatted(filter.getOperator(), fieldName, ALLOWED_OPERATORS.get(fieldType)));
        }
    }

    public static boolean isRequiringValidation(@NotNull String operator) {
        //@formatter:off
        EnumSet<FilterOperator> noValueRequiredOps = EnumSet.of(
                IS_NOT_NULL, IS_NULL, EXISTS, GLOBAL,EXPR
        );

        return noValueRequiredOps.stream()
                .map(FilterOperator::name)
                .noneMatch(op -> op.equals(operator));
        //@formatter:on
    }
}