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
    BLOB,
    BOOLEAN,
    BYTE,
    DATE,
    TIME,
    DATETIME,
    DOUBLE,
    LONG,
    DECIMAL,
    STRING
}