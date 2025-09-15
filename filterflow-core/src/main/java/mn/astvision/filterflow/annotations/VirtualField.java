package mn.astvision.filterflow.annotations;

import java.lang.annotation.*;


/**
 * @author zorigtbaatar
 * <p>
 * MongoDB-ийн entity-д виртуал талбар тодорхойлоход ашигладаг annotation.
 * <p>
 * Виртуал талбарууд нь өгөгдлийг шууд collection-д хадгалахгүйгээр
 * динамик агрегаци хийх боломжийг олгодог. Энэ annotation нь lookup, count,
 * projection, unwind, recursive graph lookup-уудыг дэмждэг.
 * </p>
 * <p>
 * Жишээ хэрэглээ:
 * <pre>{@code
 * public class IndexModel {
 *
 *     @VirtualField(
 *         fromClass = IndexGroup.class,
 *         localField = "_id",
 *         foreignField = "modelId",
 *         count = true
 *     )
 *     private long groupCount; // Холбогдсон IndexGroup-уудын тоо
 *
 *     @VirtualField(
 *         fromCollection = "questions",
 *         localField = "id",
 *         foreignField = "modelId",
 *         unwind = true,
 *         projectField = "title"
 *     )
 *     private List<String> questionTitles; // Асуултуудын гарчгийн жагсаалт
 * }
 * }</pre>
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface VirtualField {
    // --- Data source ---

    /**
     * Optional: Entity class representing the foreign collection.
     * Either fromClass or fromCollection must be provided.
     */
    Class<?> fromClass() default void.class;  // Optional entity class instead of collection name

    /**
     * Optional: Name of the foreign collection.
     * Either fromClass or fromCollection must be provided.
     */
    String fromCollection() default "";


    /**
     * Field in the local entity to match with the foreign field.
     * Default is "_id".
     */
    String localField() default "_id";

    /**
     * Field in the foreign collection to match with the local field.
     * Default is "_id".
     */
    String foreignField() default "_id";

    /**
     * Field to project from the lookup result.
     * If blank, the entire document is returned.
     */
    String projectField() default "";

    // --- Lookup behavior ---

    /**
     * If true, unwind the lookup result into separate elements.
     * Useful when mapping a list of foreign documents.
     */
    boolean unwind() default false;

    /**
     * When unwinding, preserve documents even if the array is null or empty.
     * Default is true.
     */
    boolean preserveNullAndEmptyArrays() default true;

    /**
     * If true, the virtual field will store the count (size) of the lookup array.
     * Only valid for single-level lookups.
     */
    boolean count() default false;            // If true, compute size of lookup array

    /**
     * Convert the local field to ObjectId before performing the lookup.
     * Useful when local field is stored as a string but foreign field is ObjectId.
     */
    boolean localFieldAsObjectId() default false;  // Convert local field to ObjectId


    // --- Recursive / Graph Lookup ---

    /**
     * If true, performs a $graphLookup for hierarchical relationships.
     */
    boolean recursive() default false;

    /**
     * Field name in the foreign collection representing children
     * for recursive lookups. Default is "children".
     */
    String childrenField() default "children"; // For recursive graph lookup

    // --- Custom criteria ---
    /**
     * Optional: MongoDB criteria to apply when performing the lookup.
     * Can be expressed as a JSON string or interpreted dynamically.
     * Example: "{ \"status\": \"ACTIVE\" }"
     */
    String criteria() default "";

    String expression() default "";

    // 🆕 enum support
    Class<? extends Enum<?>> enumClass() default DefaultEnum.class;

    String enumField() default "";

    enum DefaultEnum {}
}
