package mn.astvision.filterflow.builders;

import mn.astvision.filterflow.component.executors.FilterExecutor;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;
import mn.astvision.filterflow.model.FilterRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author zorigtbaatar
 */

public class SuggestionBuilder<T> {
    private final String field;
    private final FilterExecutor.Builder<T> builder;
    private FilterGroup filterGroup;
    private FilterOptions options;

    public SuggestionBuilder(String field, FilterGroup filterGroup, FilterExecutor.Builder<T> builder) {
        this.field = field;
        this.builder = builder;
        this.filterGroup = filterGroup;
    }

    public SuggestionBuilder<T> withFilters(List<FilterRequest> filters) {
        FilterGroup fg = new FilterGroup();
        fg.addComponent(filters);
        options.extractFromFilterGroup(fg);

        this.filterGroup = fg;

        if (filters == null) return this;

        boolean present = filters.stream().map(FilterRequest::getField).filter(Objects::nonNull).anyMatch(f -> f.equals("deleted"));
        if (present) return this;


        if (checkItHasDeletedField()) {
            FilterRequest eq = FilterRequest.createEq("deleted", false);
            this.filterGroup.addComponent(eq);
        }

        return this;
    }

    private boolean checkItHasDeletedField() {
        Class<T> targetType = builder.getTargetType();
        boolean hasDeletedField = false;
        Class<?> clazz = targetType;
        while (clazz != null) {
            try {
                clazz.getDeclaredField("deleted");
                hasDeletedField = true;
                break;
            } catch (NoSuchFieldException ignored) {
            }
            clazz = clazz.getSuperclass();
        }

        return hasDeletedField;
    }

    public SuggestionBuilder<T> withOptions(FilterOptions options) {
        this.options = options;
        return this;
    }

    public Set<Object> execute() {
        return builder.withFilters(filterGroup).executeSuggestionByField(field);
    }

    public Set<String> executeWithTypeString() {
        return builder.withFilters(filterGroup).executeSuggestionByFieldWithType(field);
    }

    public Set<Double> executeWithTypeDouble() {
        return builder.withFilters(filterGroup).executeSuggestionByFieldWithType(field);
    }


    public Page<Object> execute(Pageable pageable) {
        return builder.withFilters(filterGroup).executeSuggestionByField(field, pageable);
    }

    public Page<Object> execute(int limit) {
        return builder.withFilters(filterGroup).executeSuggestionByField(field, PageRequest.of(0, limit));
    }


}
