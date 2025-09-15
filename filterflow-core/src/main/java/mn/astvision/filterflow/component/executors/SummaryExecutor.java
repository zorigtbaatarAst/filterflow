package mn.astvision.filterflow.component.executors;

import mn.astvision.filterflow.component.abstraction.AbstractMongoExecutor;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import org.bson.Document;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zorigtbaatar
 */


/**
 * Flexible summary executor supporting SUM, MAX, MIN, AVG
 * on one or many fields with optional filter criteria and debug logging.
 * Uses BigDecimal for precision and formatted String outputs.
 */

public class SummaryExecutor<T> extends AbstractMongoExecutor<T> {
    private final DecimalFormat decimalFormat = new DecimalFormat("#,##0.################");

    private SummaryExecutor(MongoTemplate mongoTemplate, Class<T> targetType, FilterOptions filterOptions) {
        super(mongoTemplate, filterOptions, targetType);
    }

    public static <T> SummaryExecutor<T> create(MongoTemplate mongoTemplate, Class<T> targetType) {
        return new SummaryExecutor<T>(mongoTemplate, targetType, FilterOptions.defaults());
    }

    public static <T> SummaryExecutor<T> create(MongoTemplate mongoTemplate, Class<T> targetType, FilterOptions options) {
        return new SummaryExecutor<T>(mongoTemplate, targetType, options);
    }

    /**
     * Execute a single aggregate operation on one field.
     */
    public BigDecimal execute(String field, AggregateOp op, FilterGroup filterGroup) {
        Map<String, Map<String, BigDecimal>> result = executeMany(Map.of(field, Set.of(op)), filterGroup);

        return result.getOrDefault(field, Map.of()).getOrDefault(op.name(), BigDecimal.ZERO);
    }

    /**
     * Execute multiple aggregate operations on multiple fields.
     *
     * @param fieldOps    map of field -> set of operations
     * @param filterGroup optional filter group
     * @return map of field -> (op -> BigDecimal)
     */
    public Map<String, Map<String, BigDecimal>> executeMany(Map<String, Set<AggregateOp>> fieldOps, FilterGroup filterGroup) {
        try {
            PersistentEntity<?, ?> entity = getPersistentEntityOrThrow();

            // Validate fields
            Map<String, String> mappedFields = new LinkedHashMap<>();
            for (String field : fieldOps.keySet()) {
                PersistentProperty<?> property = entity.getPersistentProperty(field);
                if (property == null) {
                    Set<String> availableFields = getAllMappedFieldNames(entity);
                    throw new FilterException(String.format("Invalid field: '%s' for type '%s'. Allowed fields are: %s", field, targetType.getSimpleName(), availableFields));
                }
                mappedFields.put(field, property.getName());
            }

            Aggregation aggregation = buildAggregation(mappedFields, fieldOps, filterGroup);
            debug("Built aggregation pipeline: {}", aggregation.toString());

            AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, targetType, Document.class);
            Document result = results.getUniqueMappedResult();

            if (result == null) {
                debug("Aggregation returned no results for fields {}", fieldOps.keySet());
                return Collections.emptyMap();
            }

            Map<String, Map<String, BigDecimal>> finalResults = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : mappedFields.entrySet()) {
                String field = entry.getKey();
                Map<String, BigDecimal> opsResult = new LinkedHashMap<>();
                for (AggregateOp op : fieldOps.getOrDefault(field, Set.of())) {
                    String key = field + "_" + op.name().toLowerCase();
                    Object value = result.get(key);
                    BigDecimal bd = toBigDecimal(value);
                    opsResult.put(op.name(), bd);
                }
                finalResults.put(field, opsResult);
            }

            debug("Final results: {}", finalResults);
            dbExplainHandler.explainIfNeeded(aggregation);

            return finalResults;

        } catch (FilterException e) {
            throw e;
        } catch (Exception ex) {
            String msg = String.format("Failed to execute summary aggregation for fields %s on '%s': %s", fieldOps.keySet(), targetType.getSimpleName(), ex.getMessage());
            throw new FilterException(msg, ex);
        }
    }

    private Aggregation buildAggregation(
            //@formatter:off
            Map<String, String> mappedFields,
            Map<String, Set<AggregateOp>> fieldOps,
            FilterGroup filterGroup
            //@formatter:on
    ) {
        Criteria criteria = buildCriteria(filterGroup);

        List<AggregationOperation> pipeline = new ArrayList<>();
        pipeline.add(Aggregation.match(criteria));

        AggregationOperation groupOp = ctx -> {
            Document group = new Document("_id", null);
            for (Map.Entry<String, String> entry : mappedFields.entrySet()) {
                String field = entry.getKey();
                String mapped = entry.getValue();
                for (AggregateOp op : fieldOps.getOrDefault(field, Set.of())) {
                    switch (op) {
                        case SUM -> group.put(field + "_sum", new Document("$sum", "$" + mapped));
                        case MAX -> group.put(field + "_max", new Document("$max", "$" + mapped));
                        case MIN -> group.put(field + "_min", new Document("$min", "$" + mapped));
                        case AVG -> group.put(field + "_avg", new Document("$avg", "$" + mapped));
                    }
                }
            }
            return new Document("$group", group);
        };

        pipeline.add(groupOp);
        return Aggregation.newAggregation(pipeline);
    }

    private BigDecimal toBigDecimal(Object value) {
        switch (value) {
            case null -> {
                return BigDecimal.ZERO;
            }
            case BigDecimal bd -> {
                return bd;
            }
            case Number num -> {
                return new BigDecimal(num.toString());
            }
            default -> {
            }
        }
        try {
            return new BigDecimal(value.toString());
        } catch (Exception e) {
            debug("Could not convert value {} to BigDecimal", value);
            return BigDecimal.ZERO;
        }
    }

    public String format(BigDecimal value) {
        return value == null ? "0" : decimalFormat.format(value);
    }

    public enum AggregateOp {
        SUM, MAX, MIN, AVG;

        public static AggregateOp fromString(String op) {
            if (op == null) return null;
            try {
                return AggregateOp.valueOf(op.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid aggregate operation: " + op, e);
            }
        }
    }

    public static Map<String, Set<AggregateOp>> convertFieldOps(Map<String, Set<String>> fieldOps, boolean logWarnings) {
        Map<String, Set<AggregateOp>> result = new LinkedHashMap<>();

        if (fieldOps == null || fieldOps.isEmpty()) {
            return result;
        }

        fieldOps.forEach((field, opsStr) -> {
            if (field == null) {
                if (logWarnings)
                    System.out.println("Skipping null field key in fieldOps");
                return;
            }

            //@formatter:off
            Set<AggregateOp> ops = Optional.ofNullable(opsStr)
                    .orElseGet(Collections::emptySet)
                    .stream()
                    .filter(Objects::nonNull)
                    .map(opStr -> {
                        try {
                            return AggregateOp.fromString(opStr);
                        } catch (IllegalArgumentException e) {
                            if (logWarnings)
                                System.out.printf("Invalid AggregateOp '%s', falling back to %s\n" ,opStr , AggregateOp.SUM.name());
                            return AggregateOp.SUM;
                        }
                    }).collect(Collectors.toCollection(LinkedHashSet::new));
            //@formatter:on

            result.put(field, ops);
        });

        return result;
    }

}
