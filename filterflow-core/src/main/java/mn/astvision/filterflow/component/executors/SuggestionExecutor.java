package mn.astvision.filterflow.component.executors;

import mn.astvision.filterflow.component.abstraction.AbstractMongoExecutor;
import mn.astvision.filterflow.exception.FilterException;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.util.CriteriaBuilderUtil;
import org.bson.Document;
import org.springframework.data.domain.*;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author zorigtbaatar
 */

public class SuggestionExecutor<T> extends AbstractMongoExecutor<T> {
    public SuggestionExecutor(MongoTemplate mongoTemplate, FilterOptions filterOptions, Class<T> targetType) {
        super(mongoTemplate, filterOptions, targetType);
    }

    public Page<Object> suggestByField(String field, Pageable pageable, FilterGroup filterGroup) {
        try {
            // Extract pagination and sorting info from Pageable
            int limit = pageable.isPaged() ? pageable.getPageSize() : 0;
            int offset = pageable.isPaged() ? (int) pageable.getOffset() : 0;
            Sort sort = pageable.getSort();

            if (sort.isUnsorted()) {
                sort = Sort.by(Sort.Direction.ASC, field);
            }

            Set<Object> suggestions = suggestByField(field, limit, offset, sort, filterGroup);
            long total = countSuggestions(field, filterGroup);

            Pageable pageableWithSort = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(), sort);
            return new PageImpl<>(new ArrayList<>(suggestions), pageableWithSort, total);

        } catch (FilterException e) {
            throw e;
        } catch (Exception ex) {
            String msg = String.format("Failed to execute suggestion query for field '%s' on '%s': %s",
                    field, targetType.getSimpleName(), ex.getMessage());
            throw new FilterException(msg, ex);
        }
    }

    private long countSuggestions(String fieldName, FilterGroup filterGroup) {
        Aggregation countAggregation = Aggregation.newAggregation(
                Aggregation.match(
                        (filterGroup != null && !filterGroup.getComponents().isEmpty())
                                ? CriteriaBuilderUtil.buildGroupCriteria(filterGroup, FilterOptions.defaults(), targetType)
                                : new Criteria()
                ),
                Aggregation.group(fieldName),
                Aggregation.count().as("totalCount")
        );

        AggregationResults<Document> countResults = mongoTemplate.aggregate(countAggregation, targetType, Document.class);
        Document result = countResults.getUniqueMappedResult();

        if (result == null) {
            return 0L;
        }

        Object countObj = result.get("totalCount");
        if (countObj instanceof Number) {
            return ((Number) countObj).longValue();
        } else {
            // fallback, or throw an exception if unexpected
            throw new FilterException("Unexpected type for totalCount: %s".formatted(countObj != null ? countObj.getClass() : "null"));
        }
    }

    private PersistentProperty<?> resolvePropertyByPath(String fieldPath, PersistentEntity<?, ?> rootEntity) {
        String[] parts = fieldPath.split("\\.");
        PersistentEntity<?, ?> currentEntity = rootEntity;
        PersistentProperty<?> property = null;

        for (String part : parts) {
            property = currentEntity.getPersistentProperty(part);
            if (property == null) {
                return null;
            }
            Class<?> type = property.getType();
            PersistentEntity<?, ?> nestedEntity =
                    mongoTemplate.getConverter().getMappingContext().getPersistentEntity(type);
            if (nestedEntity != null) {
                currentEntity = nestedEntity;
            }
        }
        return property;
    }

    @SuppressWarnings("unchecked")
    public <V> Set<V> suggestByFieldWithType(String field, int limit, int offSet, Sort sort, FilterGroup filterGroup) {
        try {
            PersistentEntity<?, ?> entity = getPersistentEntityOrThrow();
            PersistentProperty<?> property = resolvePropertyByPath(field, entity);

            if (property == null) {
                Set<String> availableFields = getAllMappedFieldNames(entity);
                throw new FilterException(String.format(
                        "Invalid field: '%s' for type '%s'. Allowed fields are: %s",
                        field, targetType.getSimpleName(), availableFields));
            }

            Class<?> rawType = property.getType();

            Aggregation aggregation = buildSuggestionAggregation(field, limit, offSet, sort, filterGroup);
            AggregationResults<Document> results =
                    mongoTemplate.aggregate(aggregation, targetType, Document.class);

            dbExplainHandler.explainIfNeeded(aggregation);

            Set<V> suggestions = new LinkedHashSet<>();
            for (Document doc : results.getMappedResults()) {
                Object value = doc.get("_id");
                if (value != null) {
                    try {
                        suggestions.add((V) value); // cast to the property type
                    } catch (ClassCastException e) {
                        throw new FilterException("Failed to cast suggestion value '%s' to %s"
                                .formatted(value, rawType.getSimpleName()), e);
                    }
                }
            }

            return suggestions;
        } catch (FilterException e) {
            throw e;
        } catch (Exception ex) {
            String msg = String.format(
                    "Failed to execute suggestion query for field '%s' on '%s': %s",
                    field, targetType.getSimpleName(), ex.getMessage());
            throw new FilterException(msg, ex);
        }
    }


    public Set<Object> suggestByField(String field, int limit, int offSet, Sort sort, FilterGroup filterGroup) {
        try {
            PersistentEntity<?, ?> entity = getPersistentEntityOrThrow();
            PersistentProperty<?> property = resolvePropertyByPath(field, entity);

            if (property == null) {
                Set<String> availableFields = getAllMappedFieldNames(entity);
                throw new FilterException(String.format("Invalid field: '%s' for type '%s'. Allowed fields are: %s", field, targetType.getSimpleName(), availableFields));
            }

            Aggregation aggregation = buildSuggestionAggregation(field, limit, offSet, sort, filterGroup);
            dbExplainHandler.explainIfNeeded(aggregation);
            AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, targetType, Document.class);

            Set<Object> suggestions = new LinkedHashSet<>();
            for (Document doc : results.getMappedResults()) {
                suggestions.add(doc.get("_id"));
            }

            return suggestions;
        } catch (FilterException e) {
            throw e;
        } catch (Exception ex) {
            String msg = String.format("Failed to execute suggestion query for field '%s' on '%s': %s", field, targetType.getSimpleName(), ex.getMessage());
            throw new FilterException(msg, ex);
        }
    }

    private Aggregation buildSuggestionAggregation(String fieldPath, int limit, int offset, Sort sort, FilterGroup filterGroup) {
        Criteria criteria = (filterGroup != null && !filterGroup.getComponents().isEmpty())
                ? CriteriaBuilderUtil.buildGroupCriteria(filterGroup, FilterOptions.defaults(), targetType)
                : new Criteria();

        List<AggregationOperation> pipeline = new ArrayList<>();
        pipeline.add(Aggregation.match(criteria));
        pipeline.add(Aggregation.group(fieldPath).first(fieldPath).as("value"));

        if (sort == null || sort.isUnsorted()) {
            sort = Sort.by(Sort.Direction.ASC, "value");
        }

        debug("on suggestion buildAggregation, criteria: %s, fieldPath: %s, limit: %d, offset: %d, sort: %s", criteria, fieldPath, limit, offset, sort);

        pipeline.add(Aggregation.sort(sort));

        if (offset > 0) {
            pipeline.add(Aggregation.skip(offset));
        }
        if (limit > 0) {
            pipeline.add(Aggregation.limit(limit));
        }

        return Aggregation.newAggregation(pipeline);
    }
}
