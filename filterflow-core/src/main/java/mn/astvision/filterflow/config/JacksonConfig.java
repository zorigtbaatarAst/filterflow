package mn.astvision.filterflow.config;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import mn.astvision.filterflow.component.VirtualFieldAnnotationIntrospector;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer virtualFieldIntrospectorCustomizer() {
        return builder -> {
            AnnotationIntrospector defaultIntrospector = new JacksonAnnotationIntrospector();
            AnnotationIntrospector customIntrospector = new VirtualFieldAnnotationIntrospector();
            AnnotationIntrospector pair = AnnotationIntrospectorPair.pair(customIntrospector, defaultIntrospector);

            builder.annotationIntrospector(pair);
        };
    }
}
