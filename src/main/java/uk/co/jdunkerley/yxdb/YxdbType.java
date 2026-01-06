package uk.co.jdunkerley.yxdb;

/** Constants for the Alteryx YXDB file format data types. */
final class YxdbType {
    static final String BOOLEAN = "Bool";
    static final String BYTE = "Byte";
    static final String INT16 = "Int16";
    static final String INT32 = "Int32";
    static final String INT64 = "Int64";
    static final String FLOAT = "Float";
    static final String DOUBLE = "Double";
    static final String DECIMAL = "FixedDecimal";
    static final String STRING = "String";
    static final String V_STRING = "V_String";
    static final String WSTRING = "WString";
    static final String V_WSTRING = "V_WString";
    static final String DATETIME = "DateTime";
    static final String DATE = "Date";
    static final String TIME = "Time";
    static final String BLOB = "Blob";
    static final String SPATIAL_OBJ = "SpatialObj";

    static int sizeOf(YxdbField field) {
        var typeName = field.yxdbType();
        return switch (typeName) {
            case BOOLEAN -> 1;
            case BYTE -> 2;
            case INT16 -> 3;
            case V_STRING, V_WSTRING, BLOB, SPATIAL_OBJ -> 4;
            case INT32, FLOAT -> 5;
            case INT64, DOUBLE, TIME -> 9;
            case DATE -> 11;
            case DATETIME -> 20;
            case DECIMAL, STRING -> field.size() + 1;
            case WSTRING -> field.size() * 2 + 1;
            default -> throw new IllegalArgumentException("Unknown field yxdbType: " + typeName);
        };
    }

    static DataType dataTypeOf(YxdbField field) {
        return switch (field.yxdbType()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case BYTE -> DataType.BYTE;
            case INT16, INT32, INT64 -> DataType.LONG;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case DECIMAL -> DataType.DECIMAL;
            case STRING, WSTRING, V_STRING, V_WSTRING -> DataType.STRING;
            case DATE -> DataType.DATE;
            case TIME -> DataType.TIME;
            case DATETIME -> DataType.DATETIME;
            case BLOB, SPATIAL_OBJ -> DataType.BLOB;
            default -> throw new IllegalArgumentException("Unknown field yxdbType: " + field.yxdbType());
        };
    }
}
