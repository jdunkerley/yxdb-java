package uk.co.jdunkerley.yxdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

class Extractors {
    private static final DateTimeFormatter dateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static Boolean extractBoolean(ByteBuffer buffer, int start) {
        var value = buffer.get(start);
        return switch (value) {
            case 1 -> true;
            case 2 -> null;
            default -> false;
        };
    }

    static Byte extractByte(ByteBuffer buffer, int start) {
        return buffer.get(start + 1) == 1 ? null : buffer.get(start);
    }

    static Long extractInt16(ByteBuffer buffer, int start) {
        return buffer.get(start + 2) == 1 ? null : (long) buffer.getShort(start);
    }

    static Long extractInt32(ByteBuffer buffer, int start) {
        return buffer.get(start + 4) == 1 ? null : (long) buffer.getInt(start);
    }

    static Long extractInt64(ByteBuffer buffer, int start) {
        return buffer.get(start + 8) == 1 ? null : (long) buffer.getLong(start);
    }

    static Double extractFloat(ByteBuffer buffer, int start) {
        return buffer.get(start + 4) == 1 ? null : (double) buffer.getFloat(start);
    }

    static Double extractDouble(ByteBuffer buffer, int start) {
        return buffer.get(start + 8) == 1 ? null : buffer.getDouble(start);
    }

    static BigDecimal extractFixedDecimal(ByteBuffer buffer, int start, int fieldLength) {
        var str = extractString(buffer, start, fieldLength);
        return str == null ? null : new BigDecimal(str);
    }

    static LocalTime extractTime(ByteBuffer buffer, int start) {
        var str = extractString(buffer, start, 8);
        return str == null ? null : LocalTime.parse(str, DateTimeFormatter.ISO_LOCAL_TIME);
    }

    static LocalDate extractDate(ByteBuffer buffer, int start) {
        var str = extractString(buffer, start, 10);
        return str == null ? null : LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    static LocalDateTime extractDateTime(ByteBuffer buffer, int start) {
        var str = extractString(buffer, start, 19);
        return str == null ? null : LocalDateTime.parse(str, dateTime);
    }

    static String extractString(ByteBuffer buffer, int start, int fieldLength) {
        return getString(buffer, start, fieldLength, 1);
    }

    static String extractWString(ByteBuffer buffer, int start, int fieldLength) {
        return getString(buffer, start, fieldLength, 2);
    }

    private static String getString(ByteBuffer buffer, int start, int fieldLength, int charSize) {
        if (buffer.get(start + (fieldLength * charSize)) == 1) {
            return null;
        }

        // Find the position of the null terminator
        int endChar = 0;
        while (endChar < fieldLength && (buffer.get(start + (endChar * charSize)) != 0 || (charSize == 2 && buffer.get(start + (endChar * charSize) + 1) != 0))) {
            endChar++;
        }

        return new String(Arrays.copyOfRange(buffer.array(), start, endChar * charSize + start), charSize == 1 ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_16LE);
    }

    static String extractVString(ByteBuffer buffer, int start) {
        var bytes = extractBlob(buffer, start);
        return bytes == null ? null : new String(bytes, StandardCharsets.ISO_8859_1);
    }

    static String extractVWString(ByteBuffer buffer, int start) {
        var bytes = extractBlob(buffer, start);
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_16LE);
    }

    static byte[] extractBlob(ByteBuffer buffer, int start) {
        var fixedPortion = buffer.getInt(start);

        if (fixedPortion == 0) {
            return new byte[0];
        }

        if (fixedPortion == 1) {
            return null;
        }

        if (isTiny(fixedPortion)) {
            return getTinyBlob(start, buffer);
        }

        var blockStart = start + (fixedPortion & 0x7fffffff);
        var blockFirstByte = buffer.get(blockStart);
        return isSmallBlock(blockFirstByte)
                ? getSmallBlob(buffer, blockStart)
                : getNormalBlob(buffer, blockStart);
    }

    private static boolean isTiny(int fixedPortion) {
        var bitCheck1 = fixedPortion & 0x80000000;
        var bitCheck2 = fixedPortion & 0x30000000;
        return bitCheck1 == 0 && bitCheck2 != 0;
    }

    private static boolean isSmallBlock(byte value) {
        return (value & 1) == 1;
    }

    private static byte[] getNormalBlob(ByteBuffer buffer, int blockStart) {
        var blobLen = buffer.getInt(blockStart) / 2; // why divided by 2? not sure
        var blobStart = blockStart + 4;
        var blobEnd = blobStart + blobLen;
        return Arrays.copyOfRange(buffer.array(), blobStart, blobEnd);
    }

    private static byte[] getSmallBlob(ByteBuffer buffer, int blockStart) {
        var blockFirstByte = buffer.get(blockStart);
        var blobLen = unsign(blockFirstByte) >> 1;
        var blobStart = blockStart + 1;
        var blobEnd = blobStart + blobLen;
        return Arrays.copyOfRange(buffer.array(), blobStart, blobEnd);
    }

    private static byte[] getTinyBlob(int start, ByteBuffer buffer) {
        var intVal = buffer.getInt(start);
        var length = intVal >> 28;
        var end = start + length;
        return Arrays.copyOfRange(buffer.array(), start, end);
    }

    private static int unsign(byte value) {
        return value & 0xff; // Java's bytes are signed while the original algorithm is written for unsigned bytes
    }
}
