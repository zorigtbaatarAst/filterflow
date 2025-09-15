package mn.astvision.starter.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author digz6666
 */
@Slf4j
public class StringConverterTest {


    @Test
    void testStringToUTF16ConvertTest() {
        String a = "asasd asd!!";
        String b = IntStream.range(0, a.length())
                .mapToObj(i -> "\\u" + Integer.toHexString(a.charAt(i) | 0x10000).substring(1))
                .collect(Collectors.joining());

        log.info("String UTF-16 hex -> " + b);
    }
}
