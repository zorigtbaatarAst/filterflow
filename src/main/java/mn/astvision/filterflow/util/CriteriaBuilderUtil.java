package mn.astvision.filterflow.util;

import lombok.extern.slf4j.Slf4j;
import mn.astvision.filterflow.component.FilterComponent;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.handlers.OperatorHandler;
import mn.astvision.filterflow.handlers.OperatorHandlerRegistry;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import mn.astvision.filterflow.model.enums.FilterLogicMode;
import mn.astvision.filterflow.util.helpers.GlobalSearchResolver;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.*;

import static mn.astvision.filterflow.model.enums.FilterOperator.*;


/**
 * @author zorigtbaatar
 */

@Slf4j
public class CriteriaBuilderUtil {
    public static final int MAX_DEPTH = 100;

    /**
     * @param group      шүүлт хийх талбаруудын утга
     * @param targetType буцаах өгөгдлийн төрөл <T>
     * @param options    удирдлага
     * @return T төрөлд зориулсан MongoDb Criteria
     */
    public static <T> Criteria buildGroupCriteria(FilterGroup group, FilterOptions options, Class<T> targetType) {
        return buildGroupCriteria(group, options, targetType, 0);
    }

    public static <T> Criteria buildGroupCriteria(FilterGroup group, FilterOptions options, Class<T> targetType, int depth) {
        if (group.getComponents() == null || group.getComponents().isEmpty()) return new Criteria();
        if (depth > MAX_DEPTH) throw new FilterException("Maximum filter nesting depth exceeded");

        Map<FilterLogicMode, List<Criteria>> grouped = new EnumMap<>(FilterLogicMode.class);
        for (FilterLogicMode mode : FilterLogicMode.values()) {
            grouped.put(mode, new ArrayList<>());
        }

        for (FilterComponent component : group.getComponents()) {
            Criteria criteria = switch (component) {
                case FilterRequest fc -> buildSingleCriteria(fc, options, targetType);
                case FilterGroup fg -> buildGroupCriteria(fg, options, targetType, depth + 1);
                default ->
                        throw new FilterException("Unknown filter component type: %s".formatted(component.getClass().getSimpleName()));
            };
            grouped.get(component.getLogic()).add(criteria);
        }

        List<Criteria> logicGroups = Arrays.stream(FilterLogicMode.values()).map(mode -> {
            List<Criteria> list = grouped.get(mode);
            if (list.isEmpty()) return null;

            return switch (mode) {
                case AND -> new Criteria().andOperator(list.toArray(new Criteria[0]));
                case OR -> new Criteria().orOperator(list.toArray(new Criteria[0]));
                case NOR -> new Criteria().norOperator(list.toArray(new Criteria[0]));
                case NOT -> new Criteria().not().andOperator(list.toArray(new Criteria[0]));
            };
        }).filter(Objects::nonNull).toList();

        // Final merge
        return switch (logicGroups.size()) {
            case 0 -> new Criteria();
            case 1 -> logicGroups.getFirst();
            default -> new Criteria().andOperator(logicGroups.toArray(new Criteria[0]));
        };
    }

    protected static Criteria buildSingleCriteria(FilterRequest filter, FilterOptions options, Class<?> targetType) {
        String operator = filter.getOperator();
        Object value = filter.getValue();

        if (operator.equals(GLOBAL.name()))
            return GlobalSearchResolver.buildGenericSearch(value.toString(), options, targetType);
        if (operator.equals(EXPR.name())) return buildExprCriteria(value);

        if (operator.equals(CONTROL.name())) {
            throw new FilterException("CONTROL operator not allowed in buildSingleCriteria");
        }

        value = preprocessValue(filter, targetType);
        value = ConversionUtil.toMongoComparable(value);

        return buildOperatorCriteria(filter.getOperator(), filter.getField(), value);
    }

    private static Object preprocessValue(FilterRequest filter, Class<?> targetType) {
        Object value = filter.getValue();
        String operator = filter.getOperator();
        String field = filter.getField();

        Class<?> fieldType = ValidationUtil.resolveField(field, targetType);

        boolean requiresValidation = OperationUtil.isRequiringValidation(filter.getOperator());

        return switch (operator) {
            case "BETWEEN", "NOT_BETWEEN" -> ConversionUtil.convertRangeToExpectedType(value, fieldType);
            case "IN", "NOT_IN" -> {
                if (!(value instanceof Collection<?> collection)) {
                    throw new FilterException("%s operator requires a collection value.".formatted(operator));
                }
                yield collection.stream().map(item -> ConversionUtil.convertToExpectedType(item, fieldType)).toList();
            }
            default -> {
                if (requiresValidation) {
                    ValidationUtil.validateFieldAndOperation(filter, targetType);
                    yield ConversionUtil.convertToExpectedType(value, fieldType);
                } else {
                    yield value;
                }
            }
        };
    }

    private static Criteria buildOperatorCriteria(String operator, String field, Object value) {
        if (GLOBAL.equals(operator) || EXPR.equals(operator) || CONTROL.equals(operator)) {
            throw new FilterException("%s not allowed in buildOperatorCriteria".formatted(operator));
        }

        OperatorHandler handler = OperatorHandlerRegistry.get(operator);
        if (handler == null) {
            throw new FilterException("Unsupported operator: %s".formatted(operator));
        }

        return handler.build(field, value);
    }

    private static Criteria buildExprCriteria(Object value) {
        Document exprDoc = ConversionUtil.getDocumentFromObject(value);
        ValidationUtil.validateExpression(exprDoc);

        try {
            String json = exprDoc.toJson(JsonWriterSettings.builder().build());
            return Criteria.expr(MongoExpression.create(json));
        } catch (Exception e) {
            throw new FilterException("Failed to convert EXPR to MongoExpression: %s".formatted(e.getMessage()), e);
        }
    }
}


