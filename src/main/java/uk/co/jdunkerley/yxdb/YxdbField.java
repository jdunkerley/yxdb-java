package uk.co.jdunkerley.yxdb;

import java.util.function.IntSupplier;

/** Represents a field in a YXDB file.
 * @param index         The index of the field.
 * @param startPosition The start byte position of the field in the record.
 * @param name          The name of the field.
 * @param yxdbType      The YXDB data type of the field.
 * @param size          The size of the field.
 *                      For fixed-length types, this is the fixed size.
 *                      For variable-length types, this is the alwyays 4 (the size of the length prefix).
 * @param scale         The scale of the field (for decimal types).
 * @param source        The source of the field.
 * @param description   The description of the field.
 */
public record YxdbField(int index, int startPosition, String name, String yxdbType, int size, int scale, String source,
                        String description) {
    static YxdbField makeField(int index, int startPosition, String name, String type, String source, String description, IntSupplier sizeProvider, IntSupplier scaleProvider)
            throws IllegalArgumentException {
        return switch (type) {
            case YxdbType.BYTE -> new YxdbField(index, startPosition, name, YxdbType.BYTE, 1, 0, source, description);
            case YxdbType.BOOLEAN ->
                    new YxdbField(index, startPosition, name, YxdbType.BOOLEAN, 1, 0, source, description);
            case YxdbType.INT16 -> new YxdbField(index, startPosition, name, YxdbType.INT16, 2, 0, source, description);
            case YxdbType.INT32 -> new YxdbField(index, startPosition, name, YxdbType.INT32, 4, 0, source, description);
            case YxdbType.INT64 -> new YxdbField(index, startPosition, name, YxdbType.INT64, 8, 0, source, description);
            case YxdbType.FLOAT -> new YxdbField(index, startPosition, name, YxdbType.FLOAT, 4, 0, source, description);
            case YxdbType.DOUBLE ->
                    new YxdbField(index, startPosition, name, YxdbType.DOUBLE, 8, 0, source, description);
            case YxdbType.DECIMAL ->
                    new YxdbField(index, startPosition, name, YxdbType.DECIMAL, sizeProvider.getAsInt(), scaleProvider.getAsInt(), source, description);
            case YxdbType.DATE -> new YxdbField(index, startPosition, name, YxdbType.DATE, 10, 0, source, description);
            case YxdbType.TIME -> new YxdbField(index, startPosition, name, YxdbType.TIME, 8, 0, source, description);
            case YxdbType.DATETIME ->
                    new YxdbField(index, startPosition, name, YxdbType.DATETIME, 19, 0, source, description);
            case YxdbType.STRING ->
                    new YxdbField(index, startPosition, name, YxdbType.STRING, sizeProvider.getAsInt(), 0, source, description);
            case YxdbType.V_STRING ->
                    new YxdbField(index, startPosition, name, YxdbType.V_STRING, 4, 0, source, description);
            case YxdbType.WSTRING ->
                    new YxdbField(index, startPosition, name, YxdbType.WSTRING, sizeProvider.getAsInt(), 0, source, description);
            case YxdbType.V_WSTRING ->
                    new YxdbField(index, startPosition, name, YxdbType.V_WSTRING, 4, 0, source, description);
            case YxdbType.BLOB -> new YxdbField(index, startPosition, name, YxdbType.BLOB, 4, 0, source, description);
            case YxdbType.SPATIAL_OBJ ->
                    new YxdbField(index, startPosition, name, YxdbType.SPATIAL_OBJ, 4, 0, source, description);
            default -> throw new IllegalArgumentException("Unknown field YXDB type: " + type);
        };
    }

    /** Calculates the end position of the field in the record.
     * @return The end byte position of the field in the record.
     */
    public int endPosition() {
        return startPosition + YxdbType.sizeOf(this);
    }

    /** Determines if the field is of variable length.
     * @return True if the field is variable length, false otherwise.
     */
    public boolean isVariableLength() {
        return switch (yxdbType()) {
            case YxdbType.V_STRING, YxdbType.V_WSTRING, YxdbType.BLOB, YxdbType.SPATIAL_OBJ -> true;
            default -> false;
        };
    }

    /** Maps the YXDB type to a data type.
     * @return The data type of the field.
     */
    public DataType dataType() {
        return YxdbType.dataTypeOf(yxdbType());
    }
}
