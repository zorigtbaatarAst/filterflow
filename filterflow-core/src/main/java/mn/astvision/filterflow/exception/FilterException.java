package mn.astvision.filterflow.exception;

import lombok.Getter;

/**
 * @author zorigtbaatar
 */

@Getter
public class FilterException extends RuntimeException {
    private final Class<?> targetType;
    private final String hint;

    public FilterException(String message) {
        super(message);
        this.targetType = null;
        this.hint = null;
    }

    public FilterException(String message, Throwable cause) {
        super(message, cause);
        this.targetType = null;
        this.hint = null;
    }

    public FilterException(String message, Class<?> targetType, String hint) {
        super(buildMessage(message, targetType, hint));
        this.targetType = targetType;
        this.hint = hint;
    }

    private static String buildMessage(String base, Class<?> targetType, String hint) {
        StringBuilder sb = new StringBuilder(base);
        if (targetType != null) {
            sb.append(System.lineSeparator())
                    .append(" [Target: ")
                    .append(targetType.getSimpleName())
                    .append("]");
        }
        if (hint != null) {
            sb.append(System.lineSeparator()).append(" Hint: ").append(hint);
        }
        return sb.toString();
    }

}
