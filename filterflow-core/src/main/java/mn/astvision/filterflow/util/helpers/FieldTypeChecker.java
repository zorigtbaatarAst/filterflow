package mn.astvision.filterflow.util.helpers;

@FunctionalInterface
public interface FieldTypeChecker {
    boolean accept(Class<?> type);
}
