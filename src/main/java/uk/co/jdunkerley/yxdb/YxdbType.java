package uk.co.jdunkerley.yxdb;

/**
 * Constants for the Alteryx YXDB file format data types.
 */
public final class YxdbType {
    private YxdbType() {
    }

    /** Alteryx YXDB type name for boolean fields. */
    public static final String BOOLEAN = "Bool";

    /** Alteryx YXDB type name for byte fields. */
    public static final String BYTE = "Byte";

    /** Alteryx YXDB type name for 16-bit integer fields. */
    public static final String INT16 = "Int16";

    /** Alteryx YXDB type name for 32-bit integer fields. */
    public static final String INT32 = "Int32";

    /** Alteryx YXDB type name for 64-bit integer fields. */
    public static final String INT64 = "Int64";

    /** Alteryx YXDB type name for single-precision floating-point fields. */
    public static final String FLOAT = "Float";

    /** Alteryx YXDB type name for double-precision floating-point fields. */
    public static final String DOUBLE = "Double";

    /** Alteryx YXDB type name for fixed decimal fields (with a scale and precision). */
    public static final String DECIMAL = "FixedDecimal";

    /** Alteryx YXDB type name for maximum-length ISO-8859-1 string fields. */
    public static final String STRING = "String";

    /** Alteryx YXDB type name for variable-length ISO-8859-1 string fields. */
    public static final String V_STRING = "V_String";

    /** Alteryx YXDB type name for maximum-length UTF-16 string fields. */
    public static final String WSTRING = "WString";

    /** Alteryx YXDB type name for variable-length UTF-16 string fields. */
    public static final String V_WSTRING = "V_WString";

    /** Alteryx YXDB type name for local date time fields. */
    public static final String DATETIME = "DateTime";

    /** Alteryx YXDB type name for local date fields. */
    public static final String DATE = "Date";

    /** Alteryx YXDB type name for local time fields. */
    public static final String TIME = "Time";

    /** Alteryx YXDB type name for binary large object fields. */
    public static final String BLOB = "Blob";

    /** Alteryx YXDB type name for spatial object fields. */
    public static final String SPATIAL_OBJ = "SpatialObj";

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

    static DataType dataTypeOf(String yxdbType) {
        return switch (yxdbType) {
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
            default -> throw new IllegalArgumentException("Unknown field yxdbType: " + yxdbType);
        };
    }
}
