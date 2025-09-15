package mn.astvision.filterflow.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface VirtualObject {
    /**
     * Mongo collection name of the referenced object
     */
    String fromCollection() default "";

    /**
     * Entity field name in the current class that maps to foreignField
     */
    String localField();

    /**
     * Foreign field in the referenced collection (default: _id)
     */
    String foreignField() default "_id";

    /**
     * Alias for the joined object (optional)
     */
    String alias() default "";

    /**
     * Unwind the result if it's an array
     */
    boolean unwind() default false;

    /**
     * Preserve null or empty arrays when unwinding
     */
    boolean preserveNullAndEmptyArrays() default true;

    /**
     * Whether to convert localField to ObjectId
     */
    boolean localFieldAsObjectId() default false;

    String[] projectFields() default {};
}
