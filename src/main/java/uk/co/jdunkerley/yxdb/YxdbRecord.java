package uk.co.jdunkerley.yxdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

final class YxdbRecord {
    final YxdbField[] fields;
    private final Map<String, Integer> nameToIndex;

    int fixedSize;
    boolean hasVar;

    YxdbRecord(YxdbField[] fields) {
        this.fields = fields;

        hasVar = false;
        nameToIndex = new HashMap<>(fields.length);
        for (YxdbField field : fields) {
            nameToIndex.put(field.name(), field.index());
            hasVar = hasVar || field.isVariableLength();
        }

        fixedSize = fields.length == 0 ? 0 : fields[fields.length - 1].endPosition();
    }

    int mapName(String name) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throw new IllegalArgumentException("The field " + name + " does not exist.");
        }
        return index;
    }

    Boolean extractBooleanFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.BOOLEAN) {
            throw newInvalidIndex(index, "boolean");
        }
        return Extractors.extractBoolean(buffer, yxdbField.startPosition());
    }

    Byte extractByteFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.BYTE) {
            throw newInvalidIndex(index, "byte");
        }
        return Extractors.extractByte(buffer, yxdbField.startPosition());
    }

    Long extractLongFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.LONG) {
            throw newInvalidIndex(index, "int16 / int32 / int64");
        }
        return switch (yxdbField.yxdbType()) {
            case YxdbType.INT16 -> Extractors.extractInt16(buffer, yxdbField.startPosition());
            case YxdbType.INT32 -> Extractors.extractInt32(buffer, yxdbField.startPosition());
            case YxdbType.INT64 -> Extractors.extractInt64(buffer, yxdbField.startPosition());
            default ->
                    throw new IllegalArgumentException("Field at index " + index + " has invalid byteSize for long integer extraction");
        };
    }

    Double extractDoubleFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.DOUBLE) {
            throw newInvalidIndex(index, "float / double");
        }
        return switch (yxdbField.yxdbType()) {
            case YxdbType.FLOAT -> Extractors.extractFloat(buffer, yxdbField.startPosition());
            case YxdbType.DOUBLE -> Extractors.extractDouble(buffer, yxdbField.startPosition());
            default ->
                    throw new IllegalArgumentException("Field at index " + index + " has invalid byteSize for double extraction");
        };
    }

    BigDecimal extractDecimalFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.DECIMAL) {
            throw newInvalidIndex(index, "fixeddecimal");
        }
        return Extractors.extractFixedDecimal(buffer, yxdbField.startPosition(), yxdbField.size());
    }

    String extractStringFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        return switch (yxdbField.yxdbType()) {
            case YxdbType.STRING, YxdbType.DATE, YxdbType.TIME, YxdbType.DATETIME, YxdbType.DECIMAL ->
                    Extractors.extractString(buffer, yxdbField.startPosition(), yxdbField.size());
            case YxdbType.WSTRING -> Extractors.extractWString(buffer, yxdbField.startPosition(), yxdbField.size());
            case YxdbType.V_STRING -> Extractors.extractVString(buffer, yxdbField.startPosition());
            case YxdbType.V_WSTRING -> Extractors.extractVWString(buffer, yxdbField.startPosition());
            default ->
                    throw newInvalidIndex(index, "string / wstring / v_string / v_wstring / date / time / datetime / decimal");
        };
    }

    LocalDate extractDateFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.DATE) {
            throw newInvalidIndex(index, "date");
        }
        return Extractors.extractDate(buffer, yxdbField.startPosition());
    }

    LocalTime extractTimeFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.TIME) {
            throw newInvalidIndex(index, "time");
        }
        return Extractors.extractTime(buffer, yxdbField.startPosition());
    }

    LocalDateTime extractDateTimeFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.DATETIME) {
            throw newInvalidIndex(index, "datetime");
        }
        return Extractors.extractDateTime(buffer, yxdbField.startPosition());
    }

    byte[] extractBlobFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields[index];
        if (yxdbField.dataType() != DataType.BLOB) {
            throw newInvalidIndex(index, "blob / spatial");
        }
        return Extractors.extractBlob(buffer, yxdbField.startPosition());
    }

    private static IllegalArgumentException newInvalidIndex(int index, String expectedType) throws IllegalArgumentException {
        return new IllegalArgumentException("index " + index + " is not a valid index or is not a " + expectedType + " field");
    }
}
