package mn.astvision.filterflow.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import mn.astvision.filterflow.exception.FilterException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author zorigtbaatar
 */

public class ConversionUtil {

    //@formatter:off
    public static final Pattern POSSIBLE_DATE_OR_TIME_PATTERN = Pattern.compile(
            "^(" +
                    // yyyy-MM-dd or yyyy/MM/dd or dd-MM-yyyy
                    "(\\d{4}[-/]\\d{2}[-/]\\d{2}|\\d{2}-\\d{2}-\\d{4})" +
                    "(" +
                    // Optional: T or space separator
                    "[T\\s]" +
                    "\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?" +
                    "(Z|([+-]\\d{2}:\\d{2}))?" +
                    ")?" +
                    "|" +
                    // Time only: HH:mm or HH:mm:ss
                    "\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?" +
                    ")$",
            Pattern.CASE_INSENSITIVE
    );
    private static final ConcurrentMap<CacheKey, Object> conversionCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Object, Object> mongoComparableCache = new ConcurrentHashMap<>();

    private final static Logger log = LoggerFactory.getLogger(ConversionUtil.class);
    //@formatter:on
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<DateTimeFormatter> DATE_PARSERS = List.of(
            // Priority: from most to least precise
            DateTimeFormatter.ISO_INSTANT,                // 2023-08-02T10:15:30Z
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,       // 2023-08-02T10:15:30+01:00
            DateTimeFormatter.ISO_ZONED_DATE_TIME         // 2023-08-02T10:15:30+01:00[Europe/Paris]
    );
    private static final ZoneId DEFAULT_ZONE = ZoneOffset.UTC;
    ;
    //@formatter:off
    private static final List<DateTimeFormatter> LOCAL_DATE_TIME_FORMATTERS = List.of(
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    );
    private static final List<DateTimeFormatter> LOCAL_DATE_FORMATTERS = List.of(
            DateTimeFormatter.ISO_DATE,
            DateTimeFormatter.ofPattern("yyyy/MM/dd")
    );
    private static final List<DateTimeFormatter> LOCAL_TIME_FORMATTERS = List.of(
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ofPattern("HH:mm:ss")
    );
    private static final List<DateTimeFormatter> ALL_TEMPORAL_FORMATTERS = Stream.of(
            DATE_PARSERS.stream(),
            LOCAL_DATE_TIME_FORMATTERS.stream(),
            LOCAL_DATE_FORMATTERS.stream(),
            LOCAL_TIME_FORMATTERS.stream()
    ).flatMap(s -> s).distinct().toList();
    private static final Map<Class<?>, Function<String, ?>> NUMERIC_PARSERS = Map.of(
            Long.class, Long::parseLong,
            Integer.class, Integer::parseInt,
            Double.class, Double::parseDouble,
            Float.class, Float::parseFloat,
            Short.class, Short::parseShort,
            Byte.class, Byte::parseByte,
            BigDecimal.class, BigDecimal::new);
    private static final Pattern ZONED_DATE_TIME_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}:\\d{2}|Z)");
    private static final Pattern LOCAL_DATE_TIME_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?");
    private static final Pattern LOCAL_DATE_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern LOCAL_TIME_PATTERN = Pattern.compile("^\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?$");

    private static Collection<Object> createEmptyCollection(Class<?> collectionType) {
        if (collectionType.isAssignableFrom(List.class)) return new ArrayList<>();
        if (collectionType.isAssignableFrom(Set.class)) return new HashSet<>();
        if (collectionType.isAssignableFrom(Queue.class)) return new LinkedList<>();
        throw new FilterException(MessageFormat.format("Unsupported collection type: {0}", collectionType.getSimpleName()));
    }


    //@formatter:on

    private static Map<String, Object> convertToMap(Object rawValue) {
        if (rawValue instanceof Map<?, ?> inputMap) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
                Object key = entry.getKey();
                String strKey;
                if (key instanceof String) {
                    strKey = (String) key;
                } else {
                    // Optionally: throw instead of converting non-String keys
                    strKey = String.valueOf(key);
                }
                result.put(strKey, entry.getValue());
            }
            return result;
        }

        // Case 2: JSON string input
        if (rawValue instanceof String str) {
            try {
                return OBJECT_MAPPER.readValue(str, new TypeReference<>() {
                });
            } catch (Exception e) {
                throw new FilterException("Failed to parse Map from JSON string: %s".formatted(str), e);
            }
        }

        // Case 3: POJO / Java Bean
        try {
            return OBJECT_MAPPER.convertValue(rawValue, new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new FilterException("Failed to convert value to Map: %s".formatted(rawValue.getClass().getSimpleName()), e);
        }
    }

    private static Collection<Object> convertToCollection(Object rawValue, Class<?> expectedType) {
        Collection<Object> result = createEmptyCollection(expectedType);

        if (rawValue instanceof String str) {
            String[] parts = str.split(",");
            for (String part : parts) {
                result.add(part.trim()); // Keep as String or try convertToExpectedType(part, Object.class) if needed
            }
        } else if (rawValue instanceof Collection<?> inputCollection) {
            result.addAll(inputCollection);
        } else {
            throw new FilterException("Cannot convert type %s to collection".formatted(rawValue.getClass().getSimpleName()));
        }

        return result;
    }

    private static Object convertToArray(Object rawValue, Class<?> componentType) {
        if (rawValue instanceof String str) {
            String[] parts = str.split(",");
            Object array = Array.newInstance(componentType, parts.length);
            for (int i = 0; i < parts.length; i++) {
                Array.set(array, i, convertToExpectedType(parts[i].trim(), componentType));
            }
            return array;
        }

        if (rawValue instanceof Collection<?> collection) {
            Object array = Array.newInstance(componentType, collection.size());
            int i = 0;
            for (Object item : collection) {
                Array.set(array, i++, convertToExpectedType(item, componentType));
            }
            return array;
        }

        throw new FilterException("Cannot convert %s to array of %s".formatted(rawValue.getClass().getSimpleName(), componentType.getSimpleName()));
    }

    private static boolean isJson(String str) {
        return (str.startsWith("{") && str.endsWith("}")) || (str.startsWith("[") && str.endsWith("]"));
    }

    public static List<Object> convertRangeToExpectedType(Object rawValue, Class<?> expectedType) {
        List<?> rawRange = switch (rawValue) {
            case List<?> list -> list;
            case String str -> {
                try {
                    yield OBJECT_MAPPER.readValue(str, new TypeReference<List<Object>>() {
                    });
                } catch (Exception e) {
                    throw new FilterException("Failed to parse BETWEEN range from string: %s".formatted(str), e);
                }
            }
            default ->
                    throw new FilterException("BETWEEN operator requires a list or JSON array string. Found: %s".formatted(rawValue.getClass().getSimpleName()));
        };

        if (rawRange.size() != 2) {
            throw new FilterException("BETWEEN operator requires exactly 2 non-null values.");
        }

        return rawRange.stream().map(val -> convertToExpectedType(val, expectedType)).toList();
    }

    public static Object convertToExpectedType(Object rawValue, Class<?> expectedType) {
        if (rawValue == null) {
            return null;
        }

        CacheKey key = new CacheKey(rawValue, expectedType);
        if (conversionCache.containsKey(key)) {
            return conversionCache.get(key);
        }

        Object convertedValue = doConvertToExpectedType(rawValue, expectedType);
        if (convertedValue != null) {
            conversionCache.put(key, convertedValue);
        }

        return convertedValue;
    }

    public static Object doConvertToExpectedType(Object rawValue, Class<?> expectedType) {
        if (rawValue == null) return null;
        expectedType = normalize(expectedType);

        if (expectedType.isInstance(rawValue)) {
            return rawValue;
        }

        try {
            Object temporalVal = tryParseTemporal(rawValue, expectedType);
            if (temporalVal != null) {
                boolean isInstance = expectedType.isInstance(temporalVal);
                boolean isChanged = !temporalVal.getClass().equals(rawValue.getClass());

                if (isInstance || isChanged) {
                    return temporalVal;
                }
            }

            if (rawValue instanceof Number number) {
                if (expectedType == Byte.class || expectedType == byte.class) return number.byteValue();
                if (expectedType == Short.class || expectedType == short.class) return number.shortValue();
                if (expectedType == Integer.class || expectedType == int.class) return number.intValue();
                if (expectedType == Long.class || expectedType == long.class) return number.longValue();
                if (expectedType == Float.class || expectedType == float.class) return number.floatValue();
                if (expectedType == Double.class || expectedType == double.class) return number.doubleValue();
            }

            if (rawValue instanceof String str) {
                // Handle Numeric Types
                if (Number.class.isAssignableFrom(expectedType)) {
                    try {
                        return parseToNumeric(str, expectedType);
                    } catch (Exception e) {
                        throw new FilterException("Failed to parse number: '%s' to type %s".formatted(str, expectedType.getSimpleName()), e);
                    }
                }

                if (expectedType == Boolean.class) {
                    if (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false")) {
                        return Boolean.parseBoolean(str);
                    } else {
                        throw new FilterException("Invalid boolean value: '%s'".formatted(str));
                    }
                }

                // Enum
                if (expectedType.isEnum()) {
                    try {
                        return Enum.valueOf((Class<Enum>) expectedType, str);
                    } catch (IllegalArgumentException e) {
                        throw new FilterException("Invalid enum value: %s for type %s".formatted(str, expectedType.getSimpleName()));
                    } catch (ClassCastException e) {
                        throw new FilterException("Invalid enum class cast for type %s".formatted(expectedType.getSimpleName()), e);
                    }
                }

                if (isJson(str)) {
                    try {
                        return OBJECT_MAPPER.readValue(str, expectedType);
                    } catch (Exception e) {
                        throw new FilterException("Failed to parse JSON string: %s".formatted(str), e);
                    }
                }
            }

            if (Collection.class.isAssignableFrom(expectedType)) {
                return convertToCollection(rawValue, expectedType);
            }

            if (expectedType.isArray()) {
                return convertToArray(rawValue, expectedType.getComponentType());
            }

            if (Map.class.isAssignableFrom(expectedType)) {
                Map<String, Object> map = convertToMap(rawValue);
                if (!expectedType.equals(Map.class) && !expectedType.equals(LinkedHashMap.class)) {
                    return OBJECT_MAPPER.convertValue(map, expectedType);
                }
                return map;
            }

            if (!expectedType.isInstance(rawValue)) {
                throw new FilterException("Type mismatch. Cannot convert value '%s' to expected type %s".formatted(rawValue, expectedType.getSimpleName()));
            }

            return rawValue;
        } catch (FilterException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FilterException("Failed to convert value '%s' to expected type %s".formatted(rawValue, expectedType.getSimpleName()), e);
        }

    }

    protected static Object tryParseTemporal(Object value, Class<?> expectedType) {
        if (value == null || expectedType.isInstance(value)) return value;

        return switch (value) {
            case LocalDateTime ldt -> convertFromLocalDateTime(ldt, expectedType);
            case LocalDate ld -> convertFromLocalDate(ld, expectedType);
            case LocalTime lt -> convertFromLocalTime(lt, expectedType);
            case Date date -> convertFromDate(date, expectedType);
            case ZonedDateTime zdt -> convertFromZonedDateTime(zdt, expectedType);
            case OffsetDateTime odt -> convertFromOffsetDateTime(odt, expectedType);
            case Instant instant -> convertFromInstant(instant, expectedType);
            case String str when !str.isBlank() -> parseStringToTemporal(str, expectedType);
            default -> null;
        };
    }

    private static Object convertFromZonedDateTime(ZonedDateTime zdt, Class<?> expectedType) {
        if (expectedType == ZonedDateTime.class) return zdt;
        if (expectedType == OffsetDateTime.class) return zdt.toOffsetDateTime();
        if (expectedType == LocalDateTime.class) return zdt.toLocalDateTime();
        if (expectedType == LocalDate.class) return zdt.toLocalDate();
        if (expectedType == LocalTime.class) return zdt.toLocalTime();
        if (expectedType == Instant.class) return zdt.toInstant();
        if (expectedType == Date.class) return Date.from(zdt.toInstant());
        return null;
    }

    private static Object convertFromOffsetDateTime(OffsetDateTime odt, Class<?> expectedType) {
        if (expectedType == OffsetDateTime.class) return odt;
        if (expectedType == ZonedDateTime.class) return odt.toZonedDateTime();
        if (expectedType == LocalDateTime.class) return odt.toLocalDateTime();
        if (expectedType == LocalDate.class) return odt.toLocalDate();
        if (expectedType == LocalTime.class) return odt.toLocalTime();
        if (expectedType == Instant.class) return odt.toInstant();
        if (expectedType == Date.class) return Date.from(odt.toInstant());
        return null;
    }

    private static Object convertFromInstant(Instant instant, Class<?> expectedType) {
        if (expectedType == Instant.class) return instant;
        if (expectedType == ZonedDateTime.class) return instant.atZone(ZoneId.systemDefault());
        if (expectedType == OffsetDateTime.class) return instant.atOffset(ZoneOffset.UTC);
        if (expectedType == LocalDateTime.class) return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        if (expectedType == LocalDate.class)
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalDate();
        if (expectedType == LocalTime.class)
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalTime();
        if (expectedType == Date.class) return Date.from(instant);
        return null;
    }

    private static Object convertFromLocalDateTime(LocalDateTime ldt, Class<?> expectedType) {
        if (expectedType == Date.class) {
            Instant instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
            return Date.from(instant);
        }
        if (expectedType == LocalDate.class) {
            return ldt.toLocalDate();
        }
        if (expectedType == LocalTime.class) {
            return ldt.toLocalTime();
        }
        return null;
    }

    private static Object convertFromLocalDate(LocalDate ld, Class<?> expectedType) {
        if (expectedType == Date.class) {
            Instant instant = ld.atStartOfDay(ZoneId.systemDefault()).toInstant();
            return Date.from(instant);
        }
        if (expectedType == LocalDateTime.class) {
            return ld.atStartOfDay();
        }
        return null;
    }

    private static Object convertFromLocalTime(LocalTime lt, Class<?> expectedType) {
        if (expectedType == Date.class) {
            // Combine LocalTime with epoch date (1970-01-01) to create a full timestamp
            LocalDate epochDate = LocalDate.ofEpochDay(0);
            LocalDateTime ldt = LocalDateTime.of(epochDate, lt);
            Instant instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
            return Date.from(instant);
        }
        if (expectedType == LocalDateTime.class) {
            LocalDate epochDate = LocalDate.ofEpochDay(0);
            return LocalDateTime.of(epochDate, lt);
        }
        return null;
    }

    private static Object convertFromDate(Date date, Class<?> expectedType) {
        Instant instant = date.toInstant();
        if (expectedType == LocalDateTime.class) {
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }
        if (expectedType == LocalDate.class) {
            return instant.atZone(ZoneId.systemDefault()).toLocalDate();
        }
        if (expectedType == LocalTime.class) {
            return instant.atZone(ZoneId.systemDefault()).toLocalTime();
        }
        return null;
    }

    private static Object parseStringToTemporal(String str, Class<?> expectedType) {
        try {
            String typeName = expectedType.getSimpleName();
            boolean matchesPattern = POSSIBLE_DATE_OR_TIME_PATTERN.matcher(str).matches();

            if (!matchesPattern) {
                return null;
            }

            return switch (typeName) {
                case "LocalDateTime" -> tryFormats(str, LOCAL_DATE_TIME_FORMATTERS, LocalDateTime::parse);
                case "LocalDate" -> tryFormats(str, LOCAL_DATE_FORMATTERS, LocalDate::parse);
                case "LocalTime" -> tryFormats(str, LOCAL_TIME_FORMATTERS, LocalTime::parse);
                case "Date" -> tryParseToDate(str);
                default -> null;
            };
        } catch (Exception ignored) {
            return null;
        }
    }

    public static List<?> asList(Object value) {
        if (value == null) return List.of();
        if (value instanceof List<?> list) return list;
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> result = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                result.add(Array.get(value, i));
            }
            return result;
        }
        return List.of(value);
    }

    private static Object tryFormats(String str, List<DateTimeFormatter> formatters, Function2<String, DateTimeFormatter, ?> parser) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                return parser.apply(str, formatter);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    public static Date tryParseToDate(String str) {
        if (str == null || str.isBlank()) return null;

        for (DateTimeFormatter fmt : ALL_TEMPORAL_FORMATTERS) {
            try {
                TemporalAccessor parsed = fmt.parse(str);

                if (isInstant(parsed)) {
                    return Date.from(Instant.from(parsed));
                }

                if (isLocalDateTime(parsed)) {
                    return fromLocalDateTime(parsed);
                }

                if (isLocalDate(parsed)) {
                    return fromLocalDate(parsed);
                }

                if (isLocalTime(parsed)) {
                    return fromLocalTime(parsed);
                }

            } catch (DateTimeParseException ignored) {
                // Try next formatter
            }
        }

        return null;
    }

    private static boolean isInstant(TemporalAccessor ta) {
        return ta.isSupported(ChronoField.INSTANT_SECONDS);
    }

    private static boolean isLocalDateTime(TemporalAccessor ta) {
        return ta.isSupported(ChronoField.YEAR) && ta.isSupported(ChronoField.MONTH_OF_YEAR) && ta.isSupported(ChronoField.DAY_OF_MONTH) && ta.isSupported(ChronoField.HOUR_OF_DAY);
    }

    private static boolean isLocalDate(TemporalAccessor ta) {
        return ta.isSupported(ChronoField.YEAR) && ta.isSupported(ChronoField.MONTH_OF_YEAR) && ta.isSupported(ChronoField.DAY_OF_MONTH) && !ta.isSupported(ChronoField.HOUR_OF_DAY);
    }

    private static boolean isLocalTime(TemporalAccessor ta) {
        return ta.isSupported(ChronoField.HOUR_OF_DAY) && !ta.isSupported(ChronoField.YEAR);
    }

    private static Date fromLocalDateTime(TemporalAccessor ta) {
        LocalDateTime ldt = LocalDateTime.from(ta);
        return Date.from(ldt.atZone(DEFAULT_ZONE).toInstant());
    }

    private static Date fromLocalDate(TemporalAccessor ta) {
        LocalDate ld = LocalDate.from(ta);
        return Date.from(ld.atStartOfDay(DEFAULT_ZONE).toInstant());
    }

    private static Date fromLocalTime(TemporalAccessor ta) {
        LocalTime lt = LocalTime.from(ta);
        return Date.from(LocalDate.now().atTime(lt).atZone(DEFAULT_ZONE).toInstant());
    }

    private static Date parseDateString(String str) {
        //@formatter:off
        if (ZONED_DATE_TIME_PATTERN.matcher(str).matches()) {
            try {
                return Date.from(ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant());
            } catch (DateTimeException ignored) {}
        }

        if (LOCAL_DATE_TIME_PATTERN.matcher(str).matches()) {
            try {
                return Date.from(LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        .atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeException ignored) {}
        }

        if (LOCAL_DATE_PATTERN.matcher(str).matches()) {
            try {
                return Date.from(LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE)
                        .atStartOfDay(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeException ignored) {}
        }

        if (LOCAL_TIME_PATTERN.matcher(str).matches()) {
            try {
                LocalTime localTime = LocalTime.parse(str, DateTimeFormatter.ISO_LOCAL_TIME);
                LocalDate epochDate = LocalDate.ofEpochDay(0);  // 1970-01-01
                LocalDateTime dateTime = LocalDateTime.of(epochDate, localTime);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeException ignored) {}
        }

        return null;
        //@formatter:on
    }

    public static Object toMongoComparable(Object value) {
        if (value == null) return null;

        Object cached = mongoComparableCache.get(value);
        if (cached != null) {
            return cached;
        }

        Object converted = convertToMongoComparableInternal(value);
        mongoComparableCache.put(value, converted);

        return converted;
    }

    public static Object convertToMongoComparableInternal(Object value) {
        if (value instanceof String str) {
            if (POSSIBLE_DATE_OR_TIME_PATTERN.matcher(str).matches()) {
                Date parsedDate = parseDateString(str);
                if (parsedDate != null) return parsedDate;
            }
        }

        try {
            if (value instanceof LocalDateTime ldt) {
                return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
            } else if (value instanceof ZonedDateTime zdt) {
                return Date.from(zdt.toInstant());
            } else if (value instanceof LocalDate ld) {
                return Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant());
            } else if (value instanceof LocalTime lt) {
                return Date.from(LocalDate.now().atTime(lt).atZone(ZoneId.systemDefault()).toInstant());
            }
        } catch (Exception e) {
            log.error("failed on toMongoComparable at already parsed values", e);
        }

        return value;
    }

    public static boolean canParseStringToType(String str, Class<?> expectedType) {
        try {
            expectedType = normalize(expectedType);

            if (NUMERIC_PARSERS.containsKey(expectedType)) {
                NUMERIC_PARSERS.get(expectedType).apply(str);
                return true;
            }

            if (expectedType == Boolean.class) {
                return isBooleanValue(str);
            }

            if (expectedType.isEnum()) {
                return enumContains((Class<? extends Enum>) expectedType, str);
            }

            if (TemporalAccessor.class.isAssignableFrom(expectedType)) {
                return POSSIBLE_DATE_OR_TIME_PATTERN.matcher(str).matches();
            }

            return false;

        } catch (Exception e) {
            return false;
        }
    }

    private static <E extends Enum<E>> boolean enumContains(Class<E> enumClass, String value) {
        for (E constant : enumClass.getEnumConstants()) {
            if (constant.name().equals(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBooleanValue(String str) {
        return "true".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str) || "1".equals(str) || "0".equals(str);
    }

    private static boolean isArrayCompatible(Object array, Class<?> expectedComponentType) {
        if (!array.getClass().isArray()) return false;

        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            Object element = Array.get(array, i);
            if (!isCompatibleType(element, expectedComponentType)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isCollectionCompatible(Collection<?> collection) {
        for (Object element : collection) {
            if (!isCompatibleType(element, Object.class)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isStructureCompatible(Object value, Class<?> expectedType) {
        if (Collection.class.isAssignableFrom(expectedType)) {
            if (!(value instanceof Collection<?>)) return false;
            return isCollectionCompatible((Collection<?>) value);
        }

        if (expectedType.isArray()) {
            return isArrayCompatible(value, expectedType.getComponentType());
        }

        if (Map.class.isAssignableFrom(expectedType)) {
            if (!(value instanceof Map<?, ?> map)) return false;
            return isMapCompatible(map, value.getClass());
        }

        return true;
    }

    private static boolean isMapCompatible(Map<?, ?> map, Class<?> valueType) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                return false;
            }
            Object val = entry.getValue();
            if (val != null && !valueType.isInstance(val)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNullCompatible(Class<?> expectedType) {
        return !expectedType.isPrimitive();
    }

    public static boolean isCompatibleType(Object value, Class<?> expectedType) {
        if (value == null) {
            return isNullCompatible(expectedType);
        }

        expectedType = normalize(expectedType);
        Class<?> actualType = normalize(value.getClass());

        if (Number.class.isAssignableFrom(actualType) && Number.class.isAssignableFrom(expectedType)) {
            return isNumericCompatible(value, expectedType);
        }

        if (expectedType.isAssignableFrom(actualType)) {
            return isStructureCompatible(value, expectedType);
        }

        if (value instanceof String stringVal) {
            return canParseStringToType(stringVal, expectedType);
        }

        // Not compatible otherwise
        return false;
    }

    public static boolean isNumericCompatible(Object value, Class<?> expectedType) {
        if (!(value instanceof Number)) return false;

        if (expectedType == Byte.class || expectedType == byte.class) return true;
        if (expectedType == Short.class || expectedType == short.class) return true;
        if (expectedType == Integer.class || expectedType == int.class) return true;
        if (expectedType == Long.class || expectedType == long.class) return true;
        if (expectedType == Float.class || expectedType == float.class) return true;
        return expectedType == Double.class || expectedType == double.class;
    }

    protected static Class<?> normalize(Class<?> type) {
        Map<Class<?>, Class<?>> wrapperMap = Map.of(int.class, Integer.class, long.class, Long.class, double.class, Double.class, float.class, Float.class, boolean.class, Boolean.class, byte.class, Byte.class, char.class, Character.class, short.class, Short.class);
        return wrapperMap.getOrDefault(type, type);
    }

    public static Document getDocumentFromObject(Object value) {
        if (value == null) {
            throw new FilterException("EXPR value cannot be null.");
        }

        if (!(value instanceof Map<?, ?> valueMap)) {
            throw new FilterException("EXPR value must be a Map or Document. Found: %s".formatted(value.getClass().getName()));
        }

        if (valueMap.isEmpty()) {
            throw new FilterException("EXPR cannot be an empty map.");
        }

        try {
            // Convert to Document explicitly and validate structure
            Document exprDoc = new Document();
            for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new FilterException("EXPR map keys must be strings. Invalid key: %s".formatted(entry.getKey()));
                }
                exprDoc.put((String) entry.getKey(), entry.getValue());
            }

            if (exprDoc.isEmpty()) {
                throw new FilterException("EXPR parsed document is empty.");
            }

            return exprDoc;
        } catch (FilterException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FilterException("Failed to parse EXPR value into Document. Ensure it is a valid MongoDB expression.", e);
        }
    }


    public static Object parseToNumeric(String rawValue, Class<?> fieldType) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new FilterException("Cannot parse empty or null value to numeric type: %s".formatted(fieldType.getSimpleName()));
        }

        Class<?> boxedType = boxPrimitive(fieldType);
        Function<String, ?> parser = NUMERIC_PARSERS.get(boxedType);

        if (parser == null) {
            throw new FilterException("Unsupported numeric type: %s".formatted(fieldType.getSimpleName()));
        }

        try {
            return parser.apply(rawValue);
        } catch (Exception ex) {
            throw new FilterException("Invalid numeric format for type %s: '%s'".formatted(fieldType.getSimpleName(), rawValue));
        }
    }

    private static Class<?> boxPrimitive(Class<?> clazz) {
        if (!clazz.isPrimitive()) return clazz;

        return switch (clazz.getName()) {
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "double" -> Double.class;
            case "float" -> Float.class;
            case "short" -> Short.class;
            case "byte" -> Byte.class;
            default -> clazz;
        };
    }

    // Helper for functional parsing with formatters
    @FunctionalInterface
    interface Function2<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    private record CacheKey(String rawValueStr, Class<?> expectedType) {
        public CacheKey(Object rawValue, Class<?> expectedType) {
            this(rawValue == null ? "null" : rawValue.toString(), expectedType);
        }
    }

    public <S, T> T convert(S source, Class<T> targetClass) {
        try {
            T target = targetClass.getDeclaredConstructor().newInstance();
            for (Field field : source.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                try {
                    Field targetField = targetClass.getDeclaredField(field.getName());
                    targetField.setAccessible(true);
                    targetField.set(target, field.get(source));
                } catch (NoSuchFieldException ignored) {
                }
            }
            return target;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}