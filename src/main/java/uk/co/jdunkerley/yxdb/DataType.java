package uk.co.jdunkerley.yxdb;

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
    /** BLOB: a byte[] (either a Blob or SpatialObj in Alteryx) */
    BLOB,

    /** BOOLEAN: a boolean */
    BOOLEAN,

    /**  BYTE: a byte */
    BYTE,

    /** Date: a LocalDate value */
    DATE,

    /** TIME: a LocalTime value */
    TIME,

    /** DATETIME: a LocalDateTime value */
    DATETIME,

    /** DOUBLE: a double (either a Float or Double in Alteryx) */
    DOUBLE,

    /** LONG: a long (either Int16, Int32 or Int64 in Alteryx) */
    LONG,

    /** DECIMAL: a BigDecimal value */
    DECIMAL,

    /** STRING: a String value (either String, WString, V_String or V_WString in Alteryx) */
    STRING
}