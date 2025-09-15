package mn.astvision.filterflow.handlers;

import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.util.PatternCacheUtil;
import mn.astvision.filterflow.util.WildcardUtil;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static mn.astvision.filterflow.model.enums.FilterOperator.*;
import static mn.astvision.filterflow.util.ConversionUtil.asList;


/**
 * @author zorigtbaatar
 */

public class DefaultOperatorHandlers {

    public static void registerAll() {
        registerBasicComparisons();
        registerTextOperations();
        registerExistenceChecks();
        registerCollectionOperations();
        registerMapOperations();
        registerSpecialOperators();
    }

    private static void registerBasicComparisons() {
        OperatorHandlerRegistry.register("Comparison", EQUALS, (f, v) -> Criteria.where(f).is(v));
        OperatorHandlerRegistry.register("Comparison", NOT_EQUALS, (f, v) -> Criteria.where(f).ne(v));
        OperatorHandlerRegistry.register("Comparison", GREATER_THAN, (f, v) -> Criteria.where(f).gt(v));
        OperatorHandlerRegistry.register("Comparison", GREATER_THAN_EQUAL, (f, v) -> Criteria.where(f).gte(v));
        OperatorHandlerRegistry.register("Comparison", LESS_THAN, (f, v) -> Criteria.where(f).lt(v));
        OperatorHandlerRegistry.register("Comparison", LESS_THAN_EQUAL, (f, v) -> Criteria.where(f).lte(v));
        OperatorHandlerRegistry.register("Comparison", BETWEEN, (f, v) -> {
            List<?> range = asList(v);
            if (range.size() != 2) throw new FilterException("BETWEEN requires two values");

            Object start = range.get(0);
            Object end = range.get(1);

            if (start == null && end == null) {
                throw new FilterException("BETWEEN requires at least one non-null value");
            }

            Criteria criteria = Criteria.where(f);
            if (start != null) criteria = criteria.gte(start);
            if (end != null) criteria = criteria.lte(end);
            return criteria;
        });

        OperatorHandlerRegistry.register("Comparison", NOT_BETWEEN, (f, v) -> {
            List<?> range = asList(v);
            if (range.size() != 2) throw new FilterException("NOT_BETWEEN requires exactly two values");

            Object start = range.get(0);
            Object end = range.get(1);

            if (start == null && end == null) {
                throw new FilterException("NOT_BETWEEN requires at least one non-null value");
            }

            if (start != null && end != null) {
                return new Criteria().orOperator(Criteria.where(f).lt(start), Criteria.where(f).gt(end));
            }

            return start != null ? Criteria.where(f).lt(start) : Criteria.where(f).gt(end);
        });

    }

    private static void registerTextOperations() {
        OperatorHandlerRegistry.register("String Matching", LIKE, (f, v) -> {
            String str = v.toString();
            Pattern pattern = WildcardUtil.wildcardToRegex(str, true);
            return Criteria.where(f).regex(pattern);
        });

        OperatorHandlerRegistry.register("String Matching", STARTS_WITH, (f, v) -> {
            String regex = "^%s".formatted(Pattern.quote(v.toString()));
            Pattern p = PatternCacheUtil.get(regex, Pattern.CASE_INSENSITIVE);
            return Criteria.where(f).regex(p);
        });

        OperatorHandlerRegistry.register("String Matching", ENDS_WITH, (f, v) -> {
            String regex = "%s$".formatted(Pattern.quote(v.toString()));
            Pattern p = PatternCacheUtil.get(regex, Pattern.CASE_INSENSITIVE);
            return Criteria.where(f).regex(p);
        });

        OperatorHandlerRegistry.register("String Matching", CONTAINS_WORD, (f, v) -> {
            String pattern = "(^|[^\\p{L}])%s([^\\p{L}]|$)".formatted(Pattern.quote(v.toString()));
            return Criteria.where(f).regex(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE));
        });

        OperatorHandlerRegistry.register("String Matching", REGEX, (f, v) -> {
            try {
                Pattern pattern = PatternCacheUtil.get(v.toString(), Pattern.CASE_INSENSITIVE);
                return Criteria.where(f).regex(pattern);
            } catch (Exception ex) {
                throw new FilterException("Invalid regex pattern for field '%s': %s".formatted(f, v), ex);
            }
        });
    }

    private static void registerExistenceChecks() {
        OperatorHandlerRegistry.register("Existence & Null", EXISTS, (f, v) -> {
            boolean exists = Boolean.parseBoolean(v.toString());
            Criteria existsCriteria = Criteria.where(f).exists(exists);

            if (exists) {
                return new Criteria().andOperator(existsCriteria, Criteria.where(f).ne(null), new Criteria().orOperator(Criteria.where(f).not().type(4), Criteria.where(f + ".0").exists(true)));
            } else {
                return new Criteria().orOperator(existsCriteria.not(), Criteria.where(f).is(null));
            }
        });

        OperatorHandlerRegistry.register("Existence & Null", IS_NULL, (f, v) -> Criteria.where(f).is(null));
        OperatorHandlerRegistry.register("Existence & Null", IS_NOT_NULL, (f, v) -> Criteria.where(f).ne(null));
    }

    private static void registerCollectionOperations() {
        OperatorHandlerRegistry.register("Collections", IN, (f, v) -> Criteria.where(f).in(asList(v)));
        OperatorHandlerRegistry.register("Collections", NOT_IN, (f, v) -> Criteria.where(f).nin(asList(v)));
    }

    private static void registerMapOperations() {
        OperatorHandlerRegistry.register("Map-Specific", MAP_VALUE_EQUALS, (f, v) -> {
            if (!(v instanceof Map<?, ?> map) || map.size() != 1)
                throw new FilterException("MAP_VALUE_EQUALS expects a single key-value map");
            String mapKey = map.keySet().iterator().next().toString();
            Object mapVal = map.get(mapKey);
            return Criteria.where("%s.%s".formatted(f, mapKey)).is(mapVal);
        });

        OperatorHandlerRegistry.register("Map-Specific", MAP_VALUE_CONTAINS, (f, v) -> {
            if (!(v instanceof Map<?, ?> map) || map.size() != 1)
                throw new FilterException("MAP_VALUE_CONTAINS expects a single key-value map");
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) map.entrySet().iterator().next();
            String mapKey = entry.getKey().toString();
            String search = entry.getValue().toString();
            String regex = ".*%s.*".formatted(Pattern.quote(search));
            Pattern pattern = PatternCacheUtil.get(regex, Pattern.CASE_INSENSITIVE);
            return Criteria.where("%s.%s".formatted(f, mapKey)).regex(pattern);
        });

        OperatorHandlerRegistry.register("Map-Specific", MAP_VALUE_EXISTS, (f, v) -> {
            if (!(v instanceof String key)) throw new FilterException("MAP_VALUE_EXISTS expects string key");
            return Criteria.where("%s.%s".formatted(f, key)).exists(true);
        });

        OperatorHandlerRegistry.register("Map-Specific", MAP_KEY_EQUALS, (f, v) -> Criteria.where(f + "." + v.toString()).exists(true));
    }

    public static void registerSpecialOperators() {
        OperatorHandlerRegistry.register("Special", EXPR, (field, value) -> {
            throw new FilterException("EXPR operator not allowed in registerSpecialOperators");
        });

        OperatorHandlerRegistry.register("Special", GLOBAL, (field, value) -> {
            throw new FilterException("GLOBAL operator not allowed in registerSpecialOperators");
        });
    }
}
