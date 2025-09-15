package mn.astvision.filterflow.builders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zorigtbaatar
 */

public class DynamicAggregationBuilder {
    public static final DynamicAggregationBuilder builder = new DynamicAggregationBuilder();
    private static final Logger log = LoggerFactory.getLogger(DynamicAggregationBuilder.class);
    private final List<AggregationOperation> stages = new ArrayList<>();
    private final List<Criteria> matchCriteria = new ArrayList<>();
    private boolean debug = false;

    public DynamicAggregationBuilder debug(boolean debug) {
        this.debug = debug;
        return this;
    }

    public DynamicAggregationBuilder joinAndFlatten(String from, String localField, String foreignField, String as) {
        stages.add(Aggregation.lookup(from, localField, foreignField, as));
        stages.add(Aggregation.unwind(as, true));
        return this;
    }

    public DynamicAggregationBuilder matchIf(boolean condition, Criteria criteria) {
        if (condition) {
            stages.add(Aggregation.match(criteria));
        }
        return this;
    }

    public DynamicAggregationBuilder lookup(String from, String localField, String foreignField, String as) {
        stages.add(Aggregation.lookup(from, localField, foreignField, as));
        return this;
    }

    public DynamicAggregationBuilder unwind(String field, boolean preserveNull) {
        stages.add(Aggregation.unwind(field, preserveNull));
        return this;
    }

    public DynamicAggregationBuilder unwindIf(boolean condition, String field, boolean preserveNull) {
        if (condition) {
            stages.add(Aggregation.unwind(field, preserveNull));
        }
        return this;
    }

    public DynamicAggregationBuilder group(String... fields) {
        stages.add(Aggregation.group(fields));
        return this;
    }

    public DynamicAggregationBuilder groupIf(boolean condition, String... fields) {
        if (condition) {
            stages.add(Aggregation.group(fields));
        }
        return this;
    }

    public DynamicAggregationBuilder project(String... fields) {
        stages.add(Aggregation.project(fields));
        return this;
    }

    public DynamicAggregationBuilder projectIf(boolean condition, String... fields) {
        if (condition) {
            stages.add(Aggregation.project(fields));
        }
        return this;
    }

    public DynamicAggregationBuilder sortIf(boolean condition, Sort sort) {
        if (condition) {
            stages.add(Aggregation.sort(sort));
        }
        return this;
    }

    public DynamicAggregationBuilder limitIf(boolean condition, long limit) {
        if (condition) {
            stages.add(Aggregation.limit(limit));
        }
        return this;
    }

    public Aggregation build() {
        if (!matchCriteria.isEmpty()) {
            Criteria merged = new Criteria().andOperator(matchCriteria.toArray(new Criteria[0]));
            stages.addFirst(Aggregation.match(merged));
        }
        Aggregation agg = Aggregation.newAggregation(stages);

        if (debug) {
            log.info("Aggregation Pipeline:");
            stages.forEach(op -> System.out.println(op.toPipelineStages(Aggregation.DEFAULT_CONTEXT)));
        }
        return agg;
    }
}
