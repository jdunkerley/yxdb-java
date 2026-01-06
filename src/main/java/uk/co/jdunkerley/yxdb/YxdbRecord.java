package uk.co.jdunkerley.yxdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class YxdbRecord {
    private YxdbRecord(int fieldCount){
        nameToIndex = new HashMap<>(fieldCount);
        fields = new ArrayList<>(fieldCount);
    }

    public final List<YxdbField> fields;
    private final Map<String, Integer> nameToIndex;

    public int fixedSize;
    public boolean hasVar;

    private void addFieldNameToIndexMap(YxdbField field) {
        assert field.index() == fields.size();
        fields.add(field);
        nameToIndex.put(field.name(), field.index());
    }

    static YxdbRecord newFromFieldList(List<YxdbField> fields) throws IllegalArgumentException {
        YxdbRecord record = new YxdbRecord(fields.size());
        int lastRecord = 0;
        for (YxdbField field: fields) {
            record.addFieldNameToIndexMap(field);
            var fieldSize = YxdbType.sizeOf(field);
            if (fieldSize == 4) {
                record.hasVar = true;
            }
            lastRecord = field.startPosition() + fieldSize;
        }
        record.fixedSize = lastRecord;
        return record;
    }

    private int mapName(String name) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return index;
    }

    public Object extractObjectFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        return switch (yxdbField.dataType()) {
            case BOOLEAN -> extractBooleanFrom(index, buffer);
            case BYTE -> extractByteFrom(index, buffer);
            case LONG -> extractLongFrom(index, buffer);
            case DOUBLE -> extractDoubleFrom(index, buffer);
            case DECIMAL -> extractDecimalFrom(index, buffer);
            case STRING -> extractStringFrom(index, buffer);
            case DATE -> extractDateFrom(index, buffer);
            case TIME -> extractTimeFrom(index, buffer);
            case DATETIME -> extractDateTimeFrom(index, buffer);
            case BLOB -> (yxdbField.yxdbType().equals(YxdbType.SPATIAL_OBJ) ? Spatial.ToGeoJson(extractBlobFrom(index, buffer)) : extractBlobFrom(index, buffer));
        };
    }

    public Object extractObjectFrom(String name, ByteBuffer buffer) {
        return extractObjectFrom(mapName(name), buffer);
    }

    public Boolean extractBooleanFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.BOOLEAN) {
            throwInvalidIndex(index, "boolean");
        }
        return Extractors.extractBoolean(buffer, yxdbField.startPosition());
    }

    public Boolean extractBooleanFrom(String name, ByteBuffer buffer) {
        return extractBooleanFrom(mapName(name), buffer);
    }

    public Byte extractByteFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.BYTE) {
            throwInvalidIndex(index, "byte");
        }
        return Extractors.extractByte(buffer, yxdbField.startPosition());
    }

    public Byte extractByteFrom(String name, ByteBuffer buffer) {
        return extractByteFrom(mapName(name), buffer);
    }

    public Long extractLongFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.LONG) {
            throwInvalidIndex(index, "int16 / int32 / int64");
        }
        return switch (yxdbField.yxdbType()) {
            case YxdbType.INT16 -> Extractors.extractInt16(buffer, yxdbField.startPosition());
            case YxdbType.INT32 -> Extractors.extractInt32(buffer, yxdbField.startPosition());
            case YxdbType.INT64 -> Extractors.extractInt64(buffer, yxdbField.startPosition());
            default -> throw new IllegalArgumentException("Field at index " + index + " has invalid byteSize for long integer extraction");
        };
    }

    public Long extractLongFrom(String name, ByteBuffer buffer) {
        return extractLongFrom(mapName(name), buffer);
    }

    public Double extractDoubleFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.DOUBLE) {
            throwInvalidIndex(index, "float / double");
        }
        return switch (yxdbField.yxdbType()) {
            case YxdbType.FLOAT -> Extractors.extractFloat(buffer, yxdbField.startPosition());
            case YxdbType.DOUBLE -> Extractors.extractDouble(buffer, yxdbField.startPosition());
            default -> throw new IllegalArgumentException("Field at index " + index + " has invalid byteSize for double extraction");
        };
    }

    public Double extractDoubleFrom(String name, ByteBuffer buffer) {
        return extractDoubleFrom(mapName(name), buffer);
    }

    public BigDecimal extractDecimalFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.DECIMAL) {
            throwInvalidIndex(index, "fixeddecimal");
        }
        return Extractors.extractFixedDecimal(buffer, yxdbField.startPosition(), yxdbField.size());
    }

    public BigDecimal extractDecimalFrom(String name, ByteBuffer buffer) {
        return extractDecimalFrom(mapName(name), buffer);
    }

    public String extractStringFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.STRING) {
            throwInvalidIndex(index, "string / wstring / v_string / v_wstring");
        }
        return switch (yxdbField.yxdbType()) {
            case YxdbType.STRING -> Extractors.extractString(buffer, yxdbField.startPosition(), yxdbField.size());
            case YxdbType.WSTRING -> Extractors.extractWString(buffer, yxdbField.startPosition(), yxdbField.size());
            case YxdbType.V_STRING -> Extractors.extractVString(buffer, yxdbField.startPosition());
            case YxdbType.V_WSTRING -> Extractors.extractVWString(buffer, yxdbField.startPosition());
            default -> throw new IllegalArgumentException("Field at index " + index + " has invalid yxdbType for string extraction");
        };
    }

    public String extractStringFrom(String name, ByteBuffer buffer) {
        return extractStringFrom(mapName(name), buffer);
    }

    public LocalDate extractDateFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.DATE) {
            throwInvalidIndex(index, "date");
        }
        return Extractors.extractDate(buffer, yxdbField.startPosition());
    }

    public LocalDate extractDateFrom(String name, ByteBuffer buffer) {
        return extractDateFrom(mapName(name), buffer);
    }

    public LocalTime extractTimeFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.TIME) {
            throwInvalidIndex(index, "time");
        }
        return Extractors.extractTime(buffer, yxdbField.startPosition());
    }

    public LocalTime extractTimeFrom(String name, ByteBuffer buffer) {
        return extractTimeFrom(mapName(name), buffer);
    }

    public LocalDateTime extractDateTimeFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.DATETIME) {
            throwInvalidIndex(index, "datetime");
        }
        return Extractors.extractDateTime(buffer, yxdbField.startPosition());
    }

    public LocalDateTime extractDateTimeFrom(String name, ByteBuffer buffer) {
        return extractDateTimeFrom(mapName(name), buffer);
    }

    public byte[] extractBlobFrom(int index, ByteBuffer buffer) {
        var yxdbField = fields.get(index);
        if (yxdbField.dataType() != DataType.BLOB) {
            throwInvalidIndex(index, "blob / spatial");
        }
        return Extractors.extractBlob(buffer, yxdbField.startPosition());
    }

    public byte[] extractBlobFrom(String name, ByteBuffer buffer) {
        return extractBlobFrom(mapName(name), buffer);
    }

    private static void throwInvalidIndex(int index, String expectedType) throws IllegalArgumentException {
        throw new IllegalArgumentException("index " + index + " is not a valid index or is not a " + expectedType + " field");
    }

    private static void throwInvalidName(String name) throws IllegalArgumentException {
        throw new IllegalArgumentException("field " + name + " does not exist");
    }
}
