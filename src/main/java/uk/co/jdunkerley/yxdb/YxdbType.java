package uk.co.jdunkerley.yxdb;

/** Constants for the Alteryx YXDB file format data types. */
public final class YxdbType {
    public static final String BOOLEAN = "Bool";
    public static final String BYTE = "Byte";
    public static final String INT16 = "Int16";
    public static final String INT32 = "Int32";
    public static final String INT64 = "Int64";
    public static final String FLOAT = "Float";
    public static final String DOUBLE = "Double";
    public static final String DECIMAL = "Decimal";
    public static final String STRING = "String";
    public static final String WSTRING = "WString";
    public static final String DATETIME = "DateTime";
    public static final String DATE = "Date";
    public static final String TIME = "Time";
    public static final String BLOB = "Blob";
    public static final String SPATIAL_OBJ = "SpatialObj";

    static int sizeOf(MetaInfoField field) {
        var typeName = field.type();
        return switch (typeName) {
            case INT16 -> 3;
            case INT32, FLOAT -> 5;
            case INT64, DOUBLE -> 9;
            case BOOLEAN -> 1;
            case BYTE -> 2;
            case DATE -> 11;
            case TIME -> 9;
            case DATETIME -> 20;
            case DECIMAL, STRING -> field.size() + 1;
            case WSTRING -> field.size() * 2 + 1;
            case "V_String", "V_WString", BLOB, SPATIAL_OBJ -> 4;
            default -> throw new IllegalArgumentException("Unknown field type: " + typeName);
        };
    }
}
