package mn.astvision.filterflow.util;

import java.util.regex.Pattern;

/**
 * @author zorigtbaatar
 */

public class WildcardUtil {
    /**
     * Converts a wildcard pattern to a Java regex Pattern.
     * Supports:
     * - '*' matching any sequence of characters except path separator '/'
     * - '**' (if allowDeepWildcard=true) matches any sequence including '/'
     * - '?' matches any single character
     *
     * @param wildcard          Input wildcard pattern.
     * @param allowDeepWildcard If true, treat `**` as deep match (e.g., `.*`), otherwise treat all `*` as `[^/]*`.
     * @param anchor            Whether to wrap the result in ^...$ for exact match.
     * @param flags             Pattern flags (e.g., Pattern.CASE_INSENSITIVE).
     * @return Regex Pattern compiled from the wildcard.
     */
    public static Pattern wildcardToRegex(String wildcard, boolean allowDeepWildcard, boolean anchor, int flags) {
        if (wildcard == null || wildcard.isEmpty()) {
            return PatternCacheUtil.get(".*", flags); // match everything
        }

        boolean containsWildcard = wildcard.indexOf('*') >= 0 || wildcard.indexOf('?') >= 0;

        // If no wildcard, treat as literal
        if (!containsWildcard) {
            String quoted = Pattern.quote(wildcard);
            String regex = ".*" + quoted + ".*"; // contains
            if (anchor) regex = "^" + regex + "$";
            return PatternCacheUtil.get(regex, flags);
        }

        StringBuilder sb = new StringBuilder();
        if (anchor) sb.append("^");

        boolean escaping = false;
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);

            if (escaping) {
                sb.append(Pattern.quote(Character.toString(c)));
                escaping = false;
                continue;
            }

            switch (c) {
                case '\\' -> escaping = true;
                case '*' -> {
                    if (allowDeepWildcard && i + 1 < wildcard.length() && wildcard.charAt(i + 1) == '*') {
                        sb.append(".*");
                        i++; // skip next '*'
                    } else {
                        sb.append("[^/]*");
                    }
                }
                case '?' -> sb.append('.');
                case '.', '+', '(', ')', '^', '$', '{', '}', '[', ']', '|', '/' -> sb.append("\\").append(c);
                default -> sb.append(c);
            }
        }

        if (anchor) sb.append("$");

        return PatternCacheUtil.get(sb.toString(), flags);
    }

    /**
     * Overload: wildcard + deep wildcard + anchor true + case-insensitive
     */
    public static Pattern wildcardToRegex(String wildcard, boolean allowDeepWildcard) {
        return wildcardToRegex(wildcard, allowDeepWildcard, true, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Overload: wildcard + deep wildcard true + anchor true + case-insensitive
     */
    public static Pattern wildcardToRegex(String wildcard) {
        return wildcardToRegex(wildcard, true, true, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Escapes a string so it can be matched literally in a regex.
     */
    public static String escapeForRegex(String literal) {
        return Pattern.quote(literal);
    }
}

