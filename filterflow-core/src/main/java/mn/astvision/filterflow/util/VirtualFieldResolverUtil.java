package mn.astvision.filterflow.util;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Setter;
import mn.astvision.filterflow.annotations.VirtualField;
import mn.astvision.filterflow.annotations.VirtualObject;
import mn.astvision.filterflow.component.FilterContextHolder;
import mn.astvision.filterflow.util.helpers.VObjectResolver;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.lang.reflect.Field;
import java.util.*;

import static mn.astvision.filterflow.util.AggregationLogger.generateOperationInfo;

/**
 * @author zorigtbaatar
 */

public class VirtualFieldResolverUtil {
    private final Logger log = LoggerFactory.getLogger(VirtualFieldResolverUtil.class);
    @Setter
    private boolean debug;

    private VirtualFieldResolverUtil(boolean isDebug) {
        this.debug = isDebug;
    }

    public static boolean hasVirtualFields(Class<?> clazz) {
        //@formatter:off
        return Arrays.stream(clazz.getDeclaredFields())
                .anyMatch(f -> f.isAnnotationPresent(VirtualField.class) || f.isAnnotationPresent(VirtualObject.class));
        //@formatter:on

    }

    private static AggregationOperation handleEnumVirtualField(VirtualField vf, String targetField, String sourceField) {
        Class<? extends Enum<?>> enumClass = vf.enumClass();
        String enumField = vf.enumField();

        List<Document> branches = new ArrayList<>();

        for (Enum<?> constant : enumClass.getEnumConstants()) {
            try {
                String dbValue = constant.name();

                Field f = constant.getClass().getDeclaredField(enumField);
                f.setAccessible(true);
                Object displayValue = f.get(constant);

                branches.add(new Document("case", new Document("$eq", Arrays.asList("$" + sourceField, dbValue))).append("then", displayValue));

            } catch (Exception e) {
                throw new RuntimeException("Error handling enum mapping for " + constant, e);
            }
        }

        Document switchDoc = new Document("$switch", new Document("branches", branches).append("default", null));

        return context -> new Document("$addFields", new Document(targetField, switchDoc));
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static void convertLocalFieldIfNeeded(VirtualField vf, String localField, List<AggregationOperation> ops, Set<String> processedObjectIdConversions) {
        if (vf.localFieldAsObjectId() && processedObjectIdConversions.add(localField)) {
            //@formatter:off
            ops.add(ctx -> new Document("$addFields",
                    new Document(localField, new Document("$toObjectId", "$" + localField)))
            );
            //@formatter:on

        }
    }

    private static String prepareLookupLocalField(VirtualField vf, String localField, List<AggregationOperation> ops) {
        String lookupLocalField = vf.count() ? localField + "_str" : localField;
        if (vf.count()) {
            ops.add(ctx -> new Document("$addFields", new Document(lookupLocalField, new Document("$toString", "$" + localField))));
        }
        return lookupLocalField;
    }

    private static String resolveFromCollection(VirtualField vf) {
        if (!vf.fromCollection().isBlank()) return vf.fromCollection();
        if (!vf.fromClass().equals(void.class)) {
            MongoTemplate mongoTemplate = FilterContextHolder.getApplicationContext().getBean(MongoTemplate.class);
            return mongoTemplate.getCollectionName(vf.fromClass());
        }
        throw new IllegalArgumentException("@VirtualField must specify either fromCollection or fromClass");
    }

    public static VirtualFieldResolverUtil create() {
        return new VirtualFieldResolverUtil(false);
    }

    public static VirtualFieldResolverUtil create(boolean isDebug) {
        return new VirtualFieldResolverUtil(isDebug);
    }

    public static List<AggregationOperation> resolve(Class<?> clazz) {
        return create().resolveVirtualFields(clazz);
    }

    public static List<AggregationOperation> resolve(Class<?> clazz, boolean isDebug) {
        return create(isDebug).resolveVirtualFields(clazz);
    }
// --- Atomic helpers ---

    private void addUnwindAndProjection(Field field, VirtualField vf, String alias, String tempAlias, List<AggregationOperation> ops) {
        boolean isArrayField = Collection.class.isAssignableFrom(field.getType());

        Object projectionValue;

        if (!vf.expression().isBlank()) {
            Document exprDoc = Document.parse(vf.expression());

            if (isArrayField) {
                // Always wrap array expressions in $map
                Document mapDoc = new Document()
                        .append("input", "$" + tempAlias)
                        .append("as", "item")
                        .append("in", prefixFieldReferences(exprDoc, "item"));
                projectionValue = new Document("$map", mapDoc);
            } else {
                // Single object → apply expression directly
                projectionValue = prefixFieldReferences(exprDoc, tempAlias);
            }
        } else if (!vf.projectField().isBlank()) {
            projectionValue = "$" + tempAlias + "." + vf.projectField();
        } else {
            projectionValue = "$" + tempAlias;
        }

        // Add the computed field
        Document addFieldsDoc = new Document(alias, projectionValue);
        ops.add(ctx -> new Document("$addFields", addFieldsDoc));

        // Remove temp alias only if different from final alias
        if (!alias.equals(tempAlias)) {
            ops.add(Aggregation.project().andExclude(tempAlias));
        }
    }


    @SuppressWarnings("unchecked")
    private Object prefixFieldReferences(Object value, String prefix) {
        if (value instanceof String s && s.startsWith("$") && !s.startsWith("$$")) {
            // normal field reference like "$firstName" → "$tempAlias.firstName"
            return "$" + prefix + "." + s.substring(1);
        } else if (value instanceof Document doc) {
            Document newDoc = new Document();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                newDoc.put(entry.getKey(), prefixFieldReferences(entry.getValue(), prefix));
            }
            return newDoc;
        } else if (value instanceof List<?> list) {
            List<Object> newList = new ArrayList<>();
            for (Object item : list) {
                newList.add(prefixFieldReferences(item, prefix));
            }
            return newList;
        }
        return value;
    }

    private void addGraphLookup(VirtualField vf, String fromCollection, String localField, String alias, List<AggregationOperation> ops) {
        //@formatter:off
        ops.add(ctx -> new Document("$graphLookup",
                new Document("from", fromCollection)
                        .append("startWith", "$" + localField)
                        .append("connectFromField", "_id")
                        .append("connectToField", vf.childrenField())
                        .append("as", alias)));
        //@formatter:on

    }

    private void addCountField(VirtualField vf, String alias, String tempAlias, String lookupLocalField, List<AggregationOperation> ops) {
        //@formatter:off
        ops.add(Aggregation.addFields()
                .addFieldWithValue(alias, new Document("$size",
                        new Document("$ifNull", Arrays.asList("$" + tempAlias, Collections.emptyList()))
                ))
                .build());

        ops.add(Aggregation.project()
                .andExclude(tempAlias)
                .andExclude(lookupLocalField));
        //@formatter:on

    }

    private void addLookupWithOptionalCriteria(VirtualField vf, String fromCollection, String lookupLocalField, String foreignField, String tempAlias, List<AggregationOperation> ops) {
        if (!vf.criteria().isBlank()) {
            Document criteriaDoc;
            try {
                criteriaDoc = Document.parse(vf.criteria());
            } catch (Exception ex) {
                throw new IllegalArgumentException("@VirtualField criteria is invalid JSON: " + vf.criteria(), ex);
            }

            Document expr = new Document("$eq", Arrays.asList("$" + foreignField, "$$localVar"));
            Document matchExpr = new Document("$expr", criteriaDoc.isEmpty() ? expr : new Document("$and", Arrays.asList(expr, criteriaDoc)));
            List<Document> pipeline = Collections.singletonList(new Document("$match", matchExpr));

            ops.add(ctx -> new Document("$lookup",
                    //@formatter:off
                    new Document()
                            .append("from", fromCollection)
                            .append("let", new Document("localVar", "$" + lookupLocalField))
                            .append("pipeline", pipeline)
                            .append("as", tempAlias)));
                    //@formatter:on
        } else {
            ops.add(Aggregation.lookup(fromCollection, lookupLocalField, foreignField, tempAlias));
        }
    }

    /**
     * Converts a single @VirtualField into aggregation operations.
     */
    public List<AggregationOperation> resolveFieldOperator(Field field, Set<String> processedObjectIdConversions) {
        VirtualField vf = field.getAnnotation(VirtualField.class);
        if (vf == null) return Collections.emptyList();
        List<AggregationOperation> ops = new ArrayList<>();

        if (vf.enumClass() != VirtualField.DefaultEnum.class) {
            AggregationOperation enumOperation = handleEnumVirtualField(vf, field.getName(), vf.localField());
            ops.add(enumOperation);
            return ops;
        }

        String alias = field.getName();
        String tempAlias = alias + "_lookup";
        String fromCollection = resolveFromCollection(vf);
        String localField = vf.localField();
        String foreignField = vf.foreignField().isEmpty() ? "_id" : vf.foreignField();


        convertLocalFieldIfNeeded(vf, localField, ops, processedObjectIdConversions);
        String lookupLocalField = prepareLookupLocalField(vf, localField, ops);

        addLookupWithOptionalCriteria(vf, fromCollection, lookupLocalField, foreignField, tempAlias, ops);

        if (vf.count()) {
            addCountField(vf, alias, tempAlias, lookupLocalField, ops);
            return ops;
        }

        if (vf.recursive()) {
            addGraphLookup(vf, fromCollection, localField, alias, ops);
            return ops;
        }

        addUnwindAndProjection(field, vf, alias, tempAlias, ops);
        return ops;
    }

    public boolean getDebug() {
        return this.debug;
    }

    /**
     * Returns aggregation operations for all @VirtualField fields in the given class.
     */
    public List<AggregationOperation> resolveVirtualFields(Class<?> clazz) {
        List<AggregationOperation> operations = new ArrayList<>();
        Set<String> processedLocalFields = new HashSet<>();

        for (Field field : clazz.getDeclaredFields()) {
            if (field.getAnnotation(Transient.class) != null)
                continue;

            VirtualField vf = field.getAnnotation(VirtualField.class);
            if (vf != null) {
                operations.addAll(resolveFieldOperator(field, processedLocalFields));
                continue;
            }

            VirtualObject vo = field.getAnnotation(VirtualObject.class);
            if (vo != null) {
                operations.addAll(VObjectResolver.resolveObjectOperatorOptimized(field, processedLocalFields));
            }
        }

        debug("Resolved virtual fields for class ", clazz.getSimpleName());
        debug("Operation info: \n" + generateOperationInfo(operations));

        return operations;
    }

    private void debug(String message, Object... args) {
        if (!debug) return;

        if (args.length > 0) {
            log.info("[DEBUG] {} - {}", message, Arrays.toString(args));
        } else {
            log.info("[DEBUG] {}", message);
        }
    }

    public static class VirtualFieldResolutionException extends RuntimeException {
        public VirtualFieldResolutionException(String message) {
            super(message);
        }

        public VirtualFieldResolutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class ValidationUtil {

        /**
         * Validates the fields in @VirtualField annotation for aggregatability.
         * Throws VirtualFieldResolutionException if invalid.
         */
        private static void validateAggregationFields(Field field, MongoTemplate mongoTemplate) {
            VirtualField annotation = field.getAnnotation(VirtualField.class);
            if (annotation == null) {
                throw new VirtualFieldResolutionException("Field '%s' is not annotated with @VirtualField.".formatted(field.getName()));
            }

            String fromCollection = annotation.fromCollection();
            if (fromCollection.isEmpty()) {
                fromCollection = mongoTemplate.getCollectionName(field.getDeclaringClass());
            }

            String localField = annotation.localField();
            String foreignField = annotation.foreignField().isEmpty() ? "_id" : annotation.foreignField();
            String projectField = annotation.projectField();

            if (localField.isEmpty()) {
                throw new VirtualFieldResolutionException("fromCollection, localField, and projectField must be specified in @VirtualField for field '%s'".formatted(field.getName()));
            }

            MongoDatabase database = mongoTemplate.getDb();

            boolean collectionExists = database.listCollectionNames().into(new ArrayList<>()).contains(fromCollection);

            if (!collectionExists) {
                throw new VirtualFieldResolutionException("Collection '%s' specified in @VirtualField does not exist for field '%s'".formatted(fromCollection, field.getName()));
            }

            boolean localFieldExists = false;
            for (Field entityField : field.getDeclaringClass().getDeclaredFields()) {
                if (entityField.getName().equals(localField)) {
                    localFieldExists = true;
                    break;
                }
            }
            if (!localFieldExists) {
                throw new VirtualFieldResolutionException("Local field '%s' not found in entity '%s'".formatted(localField, field.getDeclaringClass().getSimpleName()));
            }

            MongoCollection<Document> foreignCollection = database.getCollection(fromCollection);
            Document foreignDoc = foreignCollection.find().limit(1).first();

            if (foreignDoc == null) {
                throw new VirtualFieldResolutionException("Foreign collection '%s' is empty, cannot validate foreign fields.".formatted(fromCollection));
            }

            if (!foreignDoc.containsKey(foreignField)) {
                throw new VirtualFieldResolutionException("Foreign field '%s' does not exist in collection '%s'".formatted(foreignField, fromCollection));
            }

            if (!foreignDoc.containsKey(projectField)) {
                throw new VirtualFieldResolutionException("Project field '%s' does not exist in collection '%s'".formatted(projectField, fromCollection));
            }
        }
    }

}
