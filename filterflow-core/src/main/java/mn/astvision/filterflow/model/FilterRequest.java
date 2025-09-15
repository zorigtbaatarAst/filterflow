package mn.astvision.filterflow.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import mn.astvision.filterflow.component.FilterComponent;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.model.enums.FilterOperator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author zorigtbaatar
 */

@Getter
public class FilterRequest extends FilterComponent {
    private FilterLogicMode logic = FilterLogicMode.AND;
    @NotBlank(message = "хүсэлтийн талбар хоосон утгатай байж болохгүй")
    private String field;
    @NotNull
    private String operator;
    @NotNull
    private Object value;

    public FilterRequest() {
    }

    public FilterRequest(String field, String operator, Object value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public FilterRequest(String field, FilterOperator operator, Object value) {
        this.field = field;
        this.operator = operator.name();
        this.value = value;
    }

    public FilterRequest(FilterLogicMode mode, String field, String operator, Object value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
        this.logic = mode;
    }

    private FilterRequest(Builder builder) {
        this.field = Objects.requireNonNull(builder.field, "Field must not be null");
        this.value = builder.value;
        this.operator = Objects.requireNonNull(builder.operator, "Operator must not be null");

        if (builder.entityClass != null) {
            validateField(builder.entityClass, this.field);
        }
    }

    private static void validateField(Class<?> entityClass, String fieldName) {
        boolean fieldExists = getAllFields(entityClass).stream().anyMatch(field -> field.getName().equals(fieldName));

        if (!fieldExists) {
            throw new IllegalArgumentException(String.format("Field '%s' does not exist in class %s", fieldName, entityClass.getSimpleName()));
        }
    }

    private static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> currentClass = type;

        while (currentClass != null && currentClass != Object.class) {
            fields.addAll(Arrays.asList(currentClass.getDeclaredFields()));
            currentClass = currentClass.getSuperclass();
        }

        return fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FilterRequest createFilterRequest(String field, FilterOperator operator, Object value) {
        return new FilterRequest(field, operator.name(), value);
    }

    public static FilterRequest createFilterRequest(FilterLogicMode mode, String field, FilterOperator operator, Object value) {
        return new FilterRequest(mode, field, operator.name(), value);
    }


    public static FilterRequest createEq(String field, Object value) {
        return new FilterRequest(field, FilterOperator.EQUALS.name(), value);
    }

    public static FilterRequest createGt(String field, Object value) {
        return new FilterRequest(field, FilterOperator.GREATER_THAN.name(), value);
    }

    public static FilterRequest createGte(String field, Object value) {
        return new FilterRequest(field, FilterOperator.GREATER_THAN_EQUAL.name(), value);
    }


    public static FilterRequest createNeq(String field, Object value) {
        return new FilterRequest(field, FilterOperator.NOT_EQUALS.name(), value);
    }


    @Override
    public String toString() {
        return "FilterRequest{" + "field='" + field + '\'' + ", operator=" + operator + ", value=" + value + '}';
    }

    public String toLogicExpression() {
        return field + FilterOperator.valueOf(operator).getLogicExpression() + value;
    }

    @Override
    public FilterLogicMode getLogic() {
        return logic;
    }


    public static class Builder {
        private String field;
        private Object value;
        private String operator;
        private Class<?> entityClass;

        public Builder field(String field) {
            this.field = Objects.requireNonNull(field, "Field must not be null");
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Builder operator(FilterOperator operator) {
            this.operator = Objects.requireNonNull(operator.name(), "Operator must not be null");
            return this;
        }

        public Builder operator(String operator) {
            this.operator = Objects.requireNonNull(operator, "Operator must not be null");
            return this;
        }


        public Builder ofType(Class<?> entityClass) {
            this.entityClass = Objects.requireNonNull(entityClass, "Entity class must not be null");
            if (this.field != null) {
                validateField(entityClass, this.field);
            }
            return this;
        }

        public FilterRequest build() {
            return new FilterRequest(this);
        }
    }


}


