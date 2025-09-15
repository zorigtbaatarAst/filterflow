package mn.astvision.filterflow.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author zorigtbaatar
 */

public class PatternCacheUtil {
    private static final int MAX_CACHE_SIZE = 500;

    private static final Map<String, Pattern> CACHE = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
            return size() > MAX_CACHE_SIZE;
        }
    };

    /**
     * Returns a cached Pattern instance for the given regex and flags.
     *
     * @param regex the regular expression string
     * @param flags pattern compilation flags (e.g., Pattern.CASE_INSENSITIVE)
     * @return compiled Pattern instance (cached)
     */
    public static Pattern get(String regex, int flags) {
        if (regex == null) {
            throw new IllegalArgumentException("Regex must not be null");
        }
        String key = buildKey(regex, flags);
        return CACHE.computeIfAbsent(key, k -> Pattern.compile(regex, flags));
    }

    /**
     * Returns a cached Pattern instance for the given regex with anchoring and flags.
     *
     * @param regex  the regex string (not already anchored)
     * @param anchor whether to apply ^...$ anchoring
     * @param flags  pattern compilation flags
     * @return compiled Pattern instance (cached)
     */
    public static Pattern getAnchored(String regex, boolean anchor, int flags) {
        if (regex == null) {
            throw new IllegalArgumentException("Regex must not be null");
        }
        String anchoredRegex = anchor ? "^" + regex + "$" : regex;
        return get(anchoredRegex, flags);
    }

    /**
     * Convenience overload with no flags (flags = 0).
     */
    public static Pattern get(String regex) {
        return get(regex, 0);
    }

    /**
     * Convenience overload for anchored regex with no flags (flags = 0).
     */
    public static Pattern getAnchored(String regex, boolean anchor) {
        return getAnchored(regex, anchor, 0);
    }

    private static String buildKey(String regex, int flags) {
        return flags + "::" + regex.intern();
    }

    /**
     * Clears the pattern cache. Useful for testing or memory-sensitive environments.
     */
    public static void clearCache() {
        CACHE.clear();
    }

    /**
     * Returns current cache size. Useful for diagnostics.
     */
    public static int cacheSize() {
        return CACHE.size();
    }
}
