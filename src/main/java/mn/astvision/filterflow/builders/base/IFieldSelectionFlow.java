package mn.astvision.filterflow.builders.base;

import java.util.Arrays;
import java.util.List;

public interface IFieldSelectionFlow<SELF> {
    List<String> getProjectionFields();

    List<String> getExcludeFields();

    default SELF withProjection(String... fields) {
        if (fields != null) getProjectionFields().addAll(Arrays.asList(fields));
        return self();
    }

    default SELF withProjection(List<String> fields) {
        if (fields != null) getProjectionFields().addAll(fields);
        return self();
    }

    default SELF withExcludeFields(String... fields) {
        if (fields != null) getExcludeFields().addAll(Arrays.asList(fields));
        return self();
    }

    default boolean hasProjection() {
        return !getProjectionFields().isEmpty();
    }

    default boolean hasExcludes() {
        return !getExcludeFields().isEmpty();
    }

    @SuppressWarnings("unchecked")
    default SELF self() {
        return (SELF) this;
    }
}
