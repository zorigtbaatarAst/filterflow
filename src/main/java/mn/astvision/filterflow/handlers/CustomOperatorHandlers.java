package mn.astvision.filterflow.handlers;

import org.springframework.data.mongodb.core.query.Criteria;

import java.util.regex.Pattern;

public class CustomOperatorHandlers {

    public static void registerAll() {
    }

    private static void registerDistanceOps() {
        OperatorHandlerRegistry.register("Distance", "DISTANCE_LESS_THAN", (field, value) -> {
            double maxDistance = Double.parseDouble(value.toString());
            return Criteria.where(field).lt(maxDistance);
        });

        OperatorHandlerRegistry.register("Numeric", "CONTAINS_NUMBER", (field, value) -> {
            String pattern = "\\d*?%s\\d*?".formatted(Pattern.quote(value.toString()));
            return Criteria.where(field).regex(Pattern.compile(pattern));
        });
    }
}
