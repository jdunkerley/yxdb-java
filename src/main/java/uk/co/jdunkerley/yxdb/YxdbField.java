package uk.co.jdunkerley.yxdb;

import java.util.function.IntSupplier;

public record YxdbField(int index, int startPosition, String name, String yxdbType, int size, int scale, String source, String description) {
    static YxdbField makeField(int index, int startPosition, String name, String type, String source, String description, IntSupplier sizeProvider, IntSupplier scaleProvider)
        throws IllegalArgumentException {
        return switch (type) {
            case YxdbType.BYTE -> new YxdbField(index, startPosition, name, YxdbType.BYTE, 1, 0, source, description);
            case YxdbType.BOOLEAN -> new YxdbField(index, startPosition, name, YxdbType.BOOLEAN, 1, 0, source, description);
            case YxdbType.INT16 -> new YxdbField(index, startPosition, name, YxdbType.INT16, 2, 0, source, description);
            case YxdbType.INT32 -> new YxdbField(index, startPosition, name, YxdbType.INT32, 4, 0, source, description);
            case YxdbType.INT64 -> new YxdbField(index, startPosition, name, YxdbType.INT64, 8, 0, source, description);
            case YxdbType.FLOAT -> new YxdbField(index, startPosition, name, YxdbType.FLOAT, 4, 0, source, description);
            case YxdbType.DOUBLE -> new YxdbField(index, startPosition, name, YxdbType.DOUBLE, 8, 0, source, description);
            case YxdbType.DECIMAL -> new YxdbField(index, startPosition, name, YxdbType.DECIMAL, sizeProvider.getAsInt(), scaleProvider.getAsInt(), source, description);
            case YxdbType.DATE -> new YxdbField(index, startPosition, name, YxdbType.DATE, 10, 0, source, description);
            case YxdbType.TIME -> new YxdbField(index, startPosition, name, YxdbType.TIME, 8, 0, source, description);
            case YxdbType.DATETIME -> new YxdbField(index, startPosition, name, YxdbType.DATETIME, 16, 0, source, description);
            case YxdbType.STRING -> new YxdbField(index, startPosition, name, YxdbType.STRING, sizeProvider.getAsInt(), 0, source, description);
            case YxdbType.V_STRING -> new YxdbField(index, startPosition, name, YxdbType.V_STRING, 4, 0, source, description);
            case YxdbType.WSTRING -> new YxdbField(index, startPosition, name, YxdbType.WSTRING, sizeProvider.getAsInt(), 0, source, description);
            case YxdbType.V_WSTRING -> new YxdbField(index, startPosition, name, YxdbType.V_WSTRING, 4, 0, source, description);
            case YxdbType.BLOB -> new YxdbField(index, startPosition, name, YxdbType.BLOB, 4, 0, source, description);
            case YxdbType.SPATIAL_OBJ -> new YxdbField(index, startPosition, name, YxdbType.SPATIAL_OBJ, 4, 0, source, description);
            default -> throw new IllegalArgumentException("Unknown field YXDB type: " + type);
        };
    }

    public DataType dataType() {
        return YxdbType.dataTypeOf(this);
    }
}
