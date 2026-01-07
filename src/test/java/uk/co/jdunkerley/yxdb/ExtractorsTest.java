package uk.co.jdunkerley.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class ExtractorsTest {
    @FunctionalInterface
    interface ExtractorFunction<T> {
        T apply(ByteBuffer buffer, int startAt);
    }

    @Test
    public void ExtractInt16() {
        ExtractorFunction<Long> extract = Extractors::extractInt16;
        Long result = extractFromBuffer(extract, 2, new byte[]{0, 0, 10, 0, 0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt16() {
        ExtractorFunction<Long> extract = Extractors::extractInt16;
        Long result = extractFromBuffer(extract, 2, new byte[]{0, 0, 10, 0, 1, 0});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractInt32() {
        ExtractorFunction<Long> extract = Extractors::extractInt32;
        Long result = extractFromBuffer(extract, 3, new byte[]{0, 0, 0, 10, 0, 0, 0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt32() {
        ExtractorFunction<Long> extract = Extractors::extractInt32;
        Long result = extractFromBuffer(extract, 3, new byte[]{0, 0, 0, 10, 0, 0, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractInt64() {
        ExtractorFunction<Long> extract = Extractors::extractInt64;
        Long result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt64() {
        ExtractorFunction<Long> extract = Extractors::extractInt64;
        Long result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractBool() {
        ExtractorFunction<Boolean> extract = Extractors::extractBoolean;
        Boolean result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0});
        Assertions.assertTrue(result);

        result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        Assertions.assertFalse(result);
    }

    @Test
    public void ExtractNullBool() {
        ExtractorFunction<Boolean> extract = Extractors::extractBoolean;
        Boolean result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractByte() {
        ExtractorFunction<Byte> extract = Extractors::extractByte;
        Byte result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0});

        Assertions.assertEquals((byte) 10, result);
    }

    @Test
    public void ExtractNullByte() {
        ExtractorFunction<Byte> extract = Extractors::extractByte;
        Byte result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractFloat() {
        ExtractorFunction<Double> extract = Extractors::extractFloat;
        Double result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, -51, -52, -116, 63, 0, 0, 0, 0, 0});

        Assertions.assertEquals(1.1f, result);
    }

    @Test
    public void ExtractNullFloat() {
        ExtractorFunction<Double> extract = Extractors::extractFloat;
        Double result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, -51, -52, -116, 63, 1, 0, 0, 0, 0});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDouble() {
        ExtractorFunction<Double> extract = Extractors::extractDouble;
        Double result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, -102, -103, -103, -103, -103, -103, -15, 63, 0});

        Assertions.assertEquals(1.1, result);
    }

    @Test
    public void ExtractNullDouble() {
        ExtractorFunction<Double> extract = Extractors::extractDouble;
        Double result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, -102, -103, -103, -103, -103, -103, -15, 63, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDate() throws ParseException {
        ExtractorFunction<LocalDate> extract = Extractors::extractDate;
        LocalDate result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0});

        Assertions.assertEquals(LocalDate.of(2021, 1, 1), result);
    }

    @Test
    public void ExtractNullDate() {
        ExtractorFunction<LocalDate> extract = Extractors::extractDate;
        LocalDate result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDateTime() throws ParseException {
        ExtractorFunction<LocalDateTime> extract = Extractors::extractDateTime;
        LocalDateTime result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 0});

        Assertions.assertEquals(LocalDateTime.of(2021, 1, 2, 3, 4, 5), result);
    }

    @Test
    public void ExtractNullDateTime() {
        ExtractorFunction<LocalDateTime> extract = Extractors::extractDateTime;
        LocalDateTime result = extractFromBuffer(extract, 4, new byte[]{0, 0, 0, 0, 50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractString(buffer, start, 15);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0});

        Assertions.assertEquals("hello world!", result);
    }

    @Test
    public void ExtractFullString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractString(buffer, start, 5);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 104, 101, 108, 108, 111, 0});

        Assertions.assertEquals("hello", result);
    }

    @Test
    public void ExtractNullString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractString(buffer, start, 5);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 104, 101, 108, 108, 111, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractEmptyString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractString(buffer, start, 5);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 0, 101, 108, 108, 111, 0});

        Assertions.assertEquals("", result);
    }

    @Test
    public void ExtractFixedDecimal() {
        ExtractorFunction<BigDecimal> extract = (buffer, start) -> Extractors.extractFixedDecimal(buffer, start, 10);
        BigDecimal result = extractFromBuffer(extract, 2, new byte[]{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0});

        Assertions.assertEquals(new BigDecimal("123.45"), result);
    }

    @Test
    public void ExtractNullFixedDecimal() {
        ExtractorFunction<BigDecimal> extract = (buffer, start) -> Extractors.extractFixedDecimal(buffer, start, 10);
        BigDecimal result = extractFromBuffer(extract, 2, new byte[]{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractWString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractWString(buffer, start, 15);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0});

        Assertions.assertEquals("hello world", result);
    }

    @Test
    public void ExtractNullWString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractWString(buffer, start, 15);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractEmptyWString() {
        ExtractorFunction<String> extract = (buffer, start) -> Extractors.extractWString(buffer, start, 15);
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 0, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0});

        Assertions.assertEquals("", result);
    }

    @Test
    public void ExtractNormalBlob() {
        // blob starts at index 6 and contains an array of 200 instances of value 66 (the character 'B')
        ExtractorFunction<byte[]> extract = Extractors::extractBlob;
        byte[] result = extractFromBuffer(extract, 6, normalBlob);
        var expected = "B".repeat(200).getBytes(StandardCharsets.ISO_8859_1);
        Assertions.assertArrayEquals(expected, result);
    }

    @Test
    public void ExtractSmallBlob() {
        // blob starts at index 6 and contains an array of 100 instances of value 66 (the character 'B')
        ExtractorFunction<byte[]> extract = Extractors::extractBlob;
        byte[] result = extractFromBuffer(extract, 6, smallBlob);
        var expected = "B".repeat(100).getBytes(StandardCharsets.ISO_8859_1);
        Assertions.assertArrayEquals(expected, result);
    }

    @Test
    public void ExtractTinyBlob() {
        // blob starts at index 6 and contains an array of 1 instance of value 1 (the character 'B')
        ExtractorFunction<byte[]> extract = Extractors::extractBlob;
        var data = new byte[]{1, 0, 65, 0, 0, 32, 66, 0, 0, 16, 0, 0, 0, 0};
        byte[] result = extractFromBuffer(extract, 6, data);
        Assertions.assertArrayEquals(new byte[]{66}, result);
    }

    @Test
    public void ExtractEmptyBlob() {
        ExtractorFunction<byte[]> extract = Extractors::extractBlob;
        byte[] result = extractFromBuffer(extract, 2, new byte[]{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertArrayEquals(new byte[]{}, result);
    }

    @Test
    public void ExtractNullBlob() {
        ExtractorFunction<byte[]> extract = Extractors::extractBlob;
        byte[] result = extractFromBuffer(extract, 2, new byte[]{0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractV_String() {
        ExtractorFunction<String> extract = Extractors::extractVString;
        String result = extractFromBuffer(extract, 6, smallBlob);

        Assertions.assertEquals("B".repeat(100), result);
    }

    @Test
    public void ExtractNullV_String() {
        ExtractorFunction<String> extract = Extractors::extractVString;
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractEmptyV_String() {
        ExtractorFunction<String> extract = Extractors::extractVString;
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals("", result);
    }

    @Test
    public void ExtractV_WString() {
        ExtractorFunction<String> extract = Extractors::extractVWString;
        String result = extractFromBuffer(extract, 2, normalBlob);

        Assertions.assertEquals("A".repeat(100), result);
    }

    @Test
    public void ExtractNullV_WString() {
        ExtractorFunction<String> extract = Extractors::extractVWString;
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractEmptyV_WString() {
        ExtractorFunction<String> extract = Extractors::extractVWString;
        String result = extractFromBuffer(extract, 2, new byte[]{0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals("", result);
    }

    private static <T> T extractFromBuffer(ExtractorFunction<T> extract, int startAt, byte[] data) {
        var buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return extract.apply(buffer, startAt);
    }

    public static byte[] smallBlob = new byte[]{1, 0, 12, 0, 0, 0, 109, 0, 0, 0, (byte) 202, 0, 0, 0, (byte) 201, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, (byte) 201, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66};
    public static byte[] normalBlob = new byte[]{1, 0, 12, 0, 0, 0, (byte) 212, 0, 0, 0, (byte) 152, 1, 0, 0, (byte) 144, 1, 0, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, 65, 0, (byte) 144, 1, 0, 0, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66};
}
