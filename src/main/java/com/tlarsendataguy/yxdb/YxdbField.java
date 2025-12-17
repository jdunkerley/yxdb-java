package com.tlarsendataguy.yxdb;

/**
 * Contains field information parsed from .yxdb metadata.
 * @param metaInfo The MetaInfoField object containing raw metadata.
 * @param startPosition The starting byte position of the field in a record.
 */
public record YxdbField(MetaInfoField metaInfo, int index, int startPosition) {
    public String name() {
        return metaInfo.name();
    }

    public String source() {
        return metaInfo.source();
    }

    public String description() {
        return metaInfo.description();
    }

    public DataType type() {
        return switch (metaInfo.type()) {
            case "Int16", "Int32", "Int64" -> DataType.LONG;
            case "Float", "Double" -> DataType.DOUBLE;
            case "FixedDecimal" -> DataType.DECIMAL;
            case "String", "WString", "V_String", "V_WString" -> DataType.STRING;
            case "Date" -> DataType.DATE;
            case "Time" -> DataType.TIME;
            case "DateTime" -> DataType.DATETIME;
            case "Bool" -> DataType.BOOLEAN;
            case "Byte" -> DataType.BYTE;
            case "Blob", "SpatialObj" -> DataType.BLOB;
            default -> throw new IllegalArgumentException("Unknown field type: " + metaInfo.type());
        };
    }

    public int size() {
        return switch (metaInfo.type()) {
            case "Int16" -> 3;
            case "Int32", "Float" -> 5;
            case "Int64", "Double" -> 9;
            case "Bool" -> 1;
            case "Byte" -> 2;
            case "Date" -> 11;
            case "Time" -> 9;
            case "DateTime" -> 20;
            case "FixedDecimal", "String", "WString", "V_String", "V_WString", "Blob", "SpatialObj" -> metaInfo.size() + 1;
            default -> throw new IllegalArgumentException("Unknown field type: " + metaInfo.type());
        };
    }

    /**
     * Fields can contain one of the following types of data. All fields types may return nulls.
     * <ul>
     *     <li>BLOB: an array of bytes</li>
     *     <li>BOOLEAN: boolean values</li>
     *     <li>BYTE: a single byte</li>
     *     <li>DATE: a date value</li>
     *     <li>TIME: a time value</li>
     *     <li>DATETIME: a date and time value</li>
     *     <li>DOUBLE: floating point numbers</li>
     *     <li>LONG: integers</li>
     *     <li>STRING: text</li>
     * </ul>
     */
    public enum DataType {
        BLOB, BOOLEAN, BYTE, DATE, TIME, DATETIME, DOUBLE, LONG, DECIMAL, STRING
    }
}
