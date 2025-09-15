package mn.astvision.filterflow.component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import mn.astvision.filterflow.annotations.VirtualField;

/**
 * @author zorigtbaatar
 */

public class VirtualFieldAnnotationIntrospector extends JacksonAnnotationIntrospector {
    @Override
    public JsonProperty.Access findPropertyAccess(Annotated annotated) {
        if (annotated.hasAnnotation(VirtualField.class)) {
            return JsonProperty.Access.READ_ONLY;
        }
        return super.findPropertyAccess(annotated);
    }
}
