package mn.astvision.filterflow.util.helpers;

import mn.astvision.filterflow.annotations.VirtualObject;
import mn.astvision.filterflow.component.FilterContextHolder;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.stream.Collectors;

public class VObjectResolver {

    private static Class<?> getTargetClass(Field field) {
        if (Collection.class.isAssignableFrom(field.getType())) {
            // If it's a collection, get the generic type
            try {
                return (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to determine generic type of collection field " + field.getName(), e);
            }
        } else {
            return field.getType();
        }
    }

    @Deprecated
    public static List<AggregationOperation> resolveObjectOperator(Field field, Set<String> processedObjectIdConversions) {
        VirtualObject vo = field.getAnnotation(VirtualObject.class);
        if (vo == null) return Collections.emptyList();

        List<AggregationOperation> ops = new ArrayList<>();
        String alias = vo.alias().isBlank() ? field.getName() : vo.alias();
        String tempAlias = alias + "_lookup";
        boolean isCollectionField = Collection.class.isAssignableFrom(field.getType());

        MongoTemplate mongoTemplate = FilterContextHolder.getApplicationContext().getBean(MongoTemplate.class);
        Class<?> targetClass = getTargetClass(field);
        String fromCollection = !vo.fromCollection().isBlank() ? vo.fromCollection() : mongoTemplate.getCollectionName(targetClass);

        String localField = vo.localField();
        String foreignField = vo.foreignField().isEmpty() ? "_id" : vo.foreignField();

        // Convert localField to ObjectId if needed
        if (vo.localFieldAsObjectId() && processedObjectIdConversions.add(localField)) {
            ops.add(ctx -> new Document("$addFields",
                    new Document(localField, new Document("$toObjectId", "$" + localField))));
        }

        // Simple lookup
        ops.add(Aggregation.lookup(fromCollection, localField, foreignField, tempAlias));

        // Transform lookup result
        ops.add(ctx -> {
            Object inExpr = isCollectionField
                    ? new Document("$map", new Document("input", "$" + tempAlias)
                    .append("as", "item")
                    .append("in", vo.projectFields().length > 0
                            ? Arrays.stream(vo.projectFields())
                            .collect(Collectors.toMap(f -> f, f -> "$$item." + f))
                            : "$$item"))
                    : (vo.projectFields().length > 0
                    ? new Document("$let",
                    new Document("vars", new Document("first", new Document("$arrayElemAt", Arrays.asList("$" + tempAlias, 0))))
                            .append("in", Arrays.stream(vo.projectFields())
                                    .collect(Collectors.toMap(f -> f, f -> "$$first." + f))))
                    : new Document("$arrayElemAt", Arrays.asList("$" + tempAlias, 0)));

            return new Document("$addFields", new Document(alias, inExpr));
        });

        // Remove temporary alias
        ops.add(Aggregation.project().andExclude(tempAlias));

        return ops;
    }

    public static List<AggregationOperation> resolveObjectOperatorOptimized(Field field, Set<String> processedObjectIdConversions) {
        VirtualObject vo = field.getAnnotation(VirtualObject.class);
        if (vo == null) return Collections.emptyList();

        List<AggregationOperation> ops = new ArrayList<>();
        String alias = vo.alias().isBlank() ? field.getName() : vo.alias();

        MongoTemplate mongoTemplate = FilterContextHolder.getApplicationContext().getBean(MongoTemplate.class);
        Class<?> targetClass = getTargetClass(field);
        String fromCollection = !vo.fromCollection().isBlank()
                ? vo.fromCollection()
                : mongoTemplate.getCollectionName(targetClass);

        String localField = vo.localField();
        String foreignField = vo.foreignField().isEmpty() ? "_id" : vo.foreignField();

        // Convert localField to ObjectId if needed
        if (vo.localFieldAsObjectId() && processedObjectIdConversions.add(localField)) {
            ops.add(ctx -> new Document("$addFields",
                    new Document(localField, new Document("$toObjectId", "$" + localField))));
        }

        // Pipeline lookup with $match and $limit 1
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(new Document("$match",
                new Document("$expr", new Document("$eq", Arrays.asList("$" + foreignField, "$$localVar")))));
        pipeline.add(new Document("$limit", 1)); // Stop after first match


        ops.add(ctx -> new Document("$lookup",
                new Document("from", fromCollection)
                        .append("let", new Document("localVar", "$" + localField))
                        .append("pipeline", pipeline)
                        .append("as", alias)));

        // Unwind single object (optional)
        ops.add(Aggregation.unwind(alias, true)); // preserveNullAndEmptyArrays = true

        // Add $project only if fields are defined
        if (vo.projectFields().length > 0) {
            Document projDoc = new Document();
            for (String f : vo.projectFields()) projDoc.append(f, 1);
            pipeline.add(new Document("$project", projDoc));
        }

        return ops;
    }

    public static List<AggregationOperation> resolveObjectOperatorBatch(
            Field field, Set<String> processedObjectIdConversions) {

        VirtualObject vo = field.getAnnotation(VirtualObject.class);
        if (vo == null) return Collections.emptyList();

        List<AggregationOperation> ops = new ArrayList<>();
        String alias = vo.alias().isBlank() ? field.getName() : vo.alias();

        MongoTemplate mongoTemplate =
                FilterContextHolder.getApplicationContext().getBean(MongoTemplate.class);
        Class<?> targetClass = getTargetClass(field);

        String fromCollection = !vo.fromCollection().isBlank()
                ? vo.fromCollection()
                : mongoTemplate.getCollectionName(targetClass);

        String localField = vo.localField();
        String foreignField = vo.foreignField().isEmpty() ? "_id" : vo.foreignField();

        // Convert localField to ObjectId if needed (only once per pipeline)
        if (vo.localFieldAsObjectId() && processedObjectIdConversions.add(localField)) {
            ops.add(ctx -> new Document("$addFields",
                    new Document(localField, new Document("$toObjectId", "$" + localField))));
        }

        ops.add(buildLookupWithArraySafe(fromCollection, localField, foreignField, vo.projectFields(), alias));

        if (!Collection.class.isAssignableFrom(field.getType())) {
            ops.add(ctx -> new Document("$addFields",
                    new Document(alias, new Document("$arrayElemAt", Arrays.asList("$" + alias, 0)))));
        }

        return ops;
    }

    private static String generateLookupKey() {
        return "lk_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static AggregationOperation buildLookupWithArraySafe(
            String fromCollection,
            String localField,
            String foreignField,
            String[] projectFields,
            String alias
    ) {
        String tempKey = generateLookupKey();

        return ctx -> new Document("$lookup",
                new Document("from", fromCollection)
                        .append("let", new Document(tempKey,
                                new Document("$cond", Arrays.asList(
                                        new Document("$isArray", "$" + localField),
                                        "$" + localField,
                                        List.of("$" + localField) // wrap scalar into array
                                ))
                        ))
                        .append("pipeline", Arrays.asList(
                                new Document("$match",
                                        new Document("$expr",
                                                new Document("$in", Arrays.asList("$" + foreignField, "$$" + tempKey))
                                        )
                                ),
                                new Document("$project", Arrays.stream(projectFields)
                                        .collect(Collectors.toMap(f -> f, f -> 1, (a, b) -> a, Document::new)))
                        ))
                        .append("as", alias)
        );
    }
}
