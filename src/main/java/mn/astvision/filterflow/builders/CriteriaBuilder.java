package mn.astvision.filterflow.builders;

import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author zorigtbaatar
 */

public class CriteriaBuilder {
    private final List<Criteria> andCriteria = new ArrayList<>();
    private final List<Criteria> orCriteria = new ArrayList<>();
    private Criteria current;

    private CriteriaBuilder() {
    }

    public static CriteriaBuilder where(String field) {
        CriteriaBuilder builder = new CriteriaBuilder();
        builder.current = Criteria.where(field);
        return builder;
    }

    public CriteriaBuilder is(Object value) {
        current = current.is(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder ne(Object value) {
        current = current.ne(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder gt(Object value) {
        current = current.gt(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder gte(Object value) {
        current = current.gte(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder lt(Object value) {
        current = current.lt(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder lte(Object value) {
        current = current.lte(value);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder regex(String pattern) {
        current = current.regex(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE));
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder in(Object... values) {
        current = current.in(values);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder notIn(Object... values) {
        current = current.nin(values);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder exists(boolean exists) {
        current = current.exists(exists);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder isNull() {
        current = current.is(null);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder isNotNull() {
        current = current.ne(null);
        andCriteria.add(current);
        return this;
    }

    public CriteriaBuilder or(String field) {
        current = Criteria.where(field);
        orCriteria.add(current);
        return this;
    }

    public CriteriaBuilder and(String field) {
        current = Criteria.where(field);
        andCriteria.add(current);
        return this;
    }

    public Criteria build() {
        Criteria root = new Criteria();
        if (!andCriteria.isEmpty() && !orCriteria.isEmpty()) {
            root.andOperator(
                    new Criteria().andOperator(andCriteria.toArray(new Criteria[0])),
                    new Criteria().orOperator(orCriteria.toArray(new Criteria[0]))
            );
        } else if (!andCriteria.isEmpty()) {
            root.andOperator(andCriteria.toArray(new Criteria[0]));
        } else if (!orCriteria.isEmpty()) {
            root.orOperator(orCriteria.toArray(new Criteria[0]));
        }
        return root;
    }
}
