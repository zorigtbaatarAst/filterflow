package mn.astvision.filterflow.util;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.util.ArrayList;
import java.util.List;

public class AggregationLogger {
    private static final Logger log = LoggerFactory.getLogger(AggregationLogger.class);

    /**
     * Logs a list of aggregation operations
     *
     * @param ops list of AggregationOperation
     */
    public static void log(List<AggregationOperation> ops) {
        if (ops == null || ops.isEmpty()) {
            log.info("[AggregationLogger] No operations to log.");
            return;
        }

        for (int i = 0; i < ops.size(); i++) {
            AggregationOperation op = ops.get(i);
            Document doc = op.toDocument(Aggregation.DEFAULT_CONTEXT);
            String type = doc.keySet().stream().findFirst().orElse("unknown");
            log.info("[AggregationLogger] Op #{} - Type: {}\n{}", i + 1, type, doc.toJson());
        }
    }

    /**
     * Logs a single AggregationOperation
     *
     * @param op single AggregationOperation
     */
    public static void log(AggregationOperation op) {
        if (op == null) {
            log.info("[AggregationLogger] Operation is null");
            return;
        }
        Document doc = op.toDocument(Aggregation.DEFAULT_CONTEXT);
        String type = doc.keySet().stream().findFirst().orElse("unknown");
        log.info("[AggregationLogger] Type: {}\n{}", type, doc.toJson());
    }

    public static String generateOperationInfo(List<AggregationOperation> ops) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < ops.size(); i++) {
            AggregationOperation op = ops.get(i);
            Document doc = op.toDocument(Aggregation.DEFAULT_CONTEXT);

            String opType = doc.keySet().stream().findFirst().orElse("unknown");
            Object value = doc.get(opType);

            List<String> fields = new ArrayList<>();

            if (value instanceof Document d) {
                fields.addAll(d.keySet());

            } else if (value instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof Document innerDoc) {
                        fields.addAll(innerDoc.keySet());
                    }
                }
            }

            sb.append(String.format("Op #%d - %s: %d, %s%n", i + 1, opType, fields.size(), fields));
        }

        return sb.toString();
    }
}
