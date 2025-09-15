package mn.astvision.filterflow.handlers;

import org.springframework.data.mongodb.core.query.Criteria;

@FunctionalInterface
public interface OperatorHandler {
    Criteria build(String field, Object value);
}
