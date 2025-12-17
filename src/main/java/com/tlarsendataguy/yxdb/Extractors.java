package com.tlarsendataguy.yxdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

class Extractors {
    private static final DateTimeFormatter time = DateTimeFormatter.ISO_LOCAL_TIME;
    private static final DateTimeFormatter date = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter dateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static Boolean extractBoolean(ByteBuffer buffer, int start) {
        var value = buffer.get(start);
        if (value == 2) {
            return null;
        }
        return buffer.get(start) == 1;
    }

    static Byte extractByte(ByteBuffer buffer, int start) {
        if (buffer.get(start+1) == 1) {
            return null;
        }
        return buffer.get(start);
    }

    static Long extractInt16(ByteBuffer buffer, int start) {
        if (buffer.get(start + 2) == 1) {
            return null;
        }
        return (long)buffer.getShort(start);
    }

    static Long extractInt32(ByteBuffer buffer, int start) {
        if (buffer.get(start + 4) == 1) {
            return null;
        }
        return (long)buffer.getInt(start);
    }

    static Long extractInt64(ByteBuffer buffer, int start) {
        if (buffer.get(start + 8) == 1) {
            return null;
        }
        return buffer.getLong(start);
    }

    static Double extractFloat(ByteBuffer buffer, int start) {
        if (buffer.get(start+4) == 1) {
            return null;
        }
        return (double)buffer.getFloat(start);
    }

    static Double extractDouble(ByteBuffer buffer, int start) {
        if (buffer.get(start+8) == 1) {
            return null;
        }
        return buffer.getDouble(start);
    }

    static BigDecimal extractFixedDecimal(ByteBuffer buffer, int start, int fieldLength) {
        if (buffer.get(start + fieldLength) == 1){
            return null;
        }
        var str = getString(buffer, start, fieldLength, 1);
        return new BigDecimal(str);
    }

    static LocalTime extractTime(ByteBuffer buffer, int start) {
        if (buffer.get(start+8) == 1) {
            return null;
        }

        var str = new String(buffer.array(), start, 8, StandardCharsets.ISO_8859_1);
        try {
            return LocalTime.parse(str, time);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }

    static LocalDate extractDate(ByteBuffer buffer, int start) {
        if (buffer.get(start+10) == 1) {
            return null;
        }
        var str = new String(buffer.array(), start, 10, StandardCharsets.ISO_8859_1);
        try {
            return LocalDate.parse(str, date);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }

    static LocalDateTime extractDateTime(ByteBuffer buffer, int start) {
        if (buffer.get(start+19) == 1) {
            return null;
        }
        var str = new String(buffer.array(), start, 19, StandardCharsets.ISO_8859_1);
        try {
            return LocalDateTime.parse(str, dateTime);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }

    static String extractString(ByteBuffer buffer, int start, int fieldLength) {
        if (buffer.get(start+fieldLength) == 1) {
            return null;
        }
        return getString(buffer, start, fieldLength, 1);
    }

    static String extractWString(ByteBuffer buffer, int start, int fieldLength) {
        if (buffer.get(start + (fieldLength * 2)) == 1) {
            return null;
        }
        return getString(buffer, start, fieldLength, 2);
    }

    static String extractVString(ByteBuffer buffer, int start) {
        var bytes = parseBlob(buffer, start);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    static String extractVWString(ByteBuffer buffer, int start) {
        var bytes = parseBlob(buffer, start);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_16LE);
    }

    static byte[] extractBlob(ByteBuffer buffer, int start) {
        return parseBlob(buffer, start);
    }

    private static byte[] parseBlob(ByteBuffer buffer, int start) {
        var fixedPortion = buffer.getInt(start);
        if (fixedPortion == 0) {
            return new byte[]{};
        }
        if (fixedPortion == 1) {
            return null;
        }

        if (isTiny(fixedPortion)) {
            return getTinyBlob(start, buffer);
        }

        var blockStart = start + (fixedPortion & 0x7fffffff);
        var blockFirstByte = buffer.get(blockStart);
        if (isSmallBlock(blockFirstByte)) {
            return getSmallBlob(buffer, blockStart);
        }
        return getNormalBlob(buffer, blockStart);
    }

    private static String getString(ByteBuffer buffer, int start, int fieldLength, int charSize) {
        int end = getEndOfStringPos(buffer.array(), start, fieldLength, charSize);
        if (charSize == 1) {
            return new String(Arrays.copyOfRange(buffer.array(), start, end), StandardCharsets.ISO_8859_1);
        }
        return new String(Arrays.copyOfRange(buffer.array(), start, end), StandardCharsets.UTF_16LE);
    }

    private static int getEndOfStringPos(byte[] buffer, int start, int fieldLength, int charSize) {
        int fieldTo = start + (fieldLength * charSize);
        int strLen = 0;
        for (var i = start; i < fieldTo; i=i+charSize) {
            if (buffer[i] == 0 && buffer[i+(charSize-1)] == 0) {
                break;
            }
            strLen++;
        }
        return start+(strLen * charSize);
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
