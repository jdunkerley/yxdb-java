package uk.co.jdunkerley.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class YxdbRecordTest {
    @Test
    public void TestReadInt16Record() {
        var record = loadRecordWithValueColumn("Int16", 2);
        var source = wrap(new byte[]{23, 0, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("Int16", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.LONG, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(3, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadInt32Record() {
        var record = loadRecordWithValueColumn("Int32", 4);
        var source = wrap(new byte[]{23, 0, 0, 0, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("Int32", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.LONG, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(5, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadInt64Record() {
        var record = loadRecordWithValueColumn("Int64", 8);
        var source = wrap(new byte[]{23, 0, 0, 0, 0, 0, 0, 0, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("Int64", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.LONG, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(9, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadFloatRecord() {
        var record = loadRecordWithValueColumn("Float", 4);
        var source = wrap(new byte[]{-51, -52, -116, 63, 0, 0, 0, 0, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("Float", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.DOUBLE, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(1.1f, record.extractDoubleFrom(0, source));
        Assertions.assertEquals(5, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadDoubleRecord() {
        var record = loadRecordWithValueColumn("Double", 8);
        var source = wrap(new byte[]{-102, -103, -103, -103, -103, -103, -15, 63, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("Double", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.DOUBLE, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(1.1, record.extractDoubleFrom(0, source));
        Assertions.assertEquals(9, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadFixedDecimalRecord() {
        var record = loadRecordWithValueColumn("FixedDecimal", 10);
        var source = wrap(new byte[]{49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("FixedDecimal", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.DECIMAL, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals(new BigDecimal("123.45"), record.extractDecimalFrom(0, source));
        Assertions.assertEquals("123.45", record.extractStringFrom(0, source));
        Assertions.assertEquals(11, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadStringRecord() {
        var record = loadRecordWithValueColumn("String", 15);
        var source = wrap(new byte[]{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("String", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.STRING, record.fields[0].dataType());
        Assertions.assertEquals(15, record.fields[0].size());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals("hello world!", record.extractStringFrom(0, source));
        Assertions.assertEquals(16, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadWString() {
        var record = loadRecordWithValueColumn("WString", 15);
        var source = wrap(new byte[]{104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33, 0, 0, 0, 23, 0, 77, 0, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("WString", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.STRING, record.fields[0].dataType());
        Assertions.assertEquals(15, record.fields[0].size());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals("hello world!", record.extractStringFrom(0, source));
        Assertions.assertEquals(31, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadV_String() {
        // Size is ignored for V_WString
        var record = loadRecordWithValueColumn("V_String", 15);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("V_String", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.STRING, record.fields[0].dataType());
        Assertions.assertEquals(4, record.fields[0].size());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals("", record.extractStringFrom(0, source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadV_WString() {
        // Size is ignored for V_WString
        var record = loadRecordWithValueColumn("V_WString", 15);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertEquals("V_WString", record.fields[0].yxdbType());
        Assertions.assertSame(DataType.STRING, record.fields[0].dataType());
        Assertions.assertEquals(4, record.fields[0].size());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(0, record.mapName("value"));
        Assertions.assertEquals("", record.extractStringFrom(0, source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadDate() throws ParseException {
        var record = loadRecordWithValueColumn("Date", 10);
        var source = wrap(new byte[]{50, 48, 50, 49, 45, 48, 49, 45, 48, 49, 0});

        var expected = LocalDate.of(2021, 1, 1);

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.DATE, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(expected, record.extractDateFrom(0, source));
        Assertions.assertEquals("2021-01-01", record.extractStringFrom(0, source));
        Assertions.assertEquals(11, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadTime() throws ParseException {
        var record = loadRecordWithValueColumn("Time", 8);
        var source = wrap(new byte[]{48, 51, 58, 48, 52, 58, 48, 53, 0});

        var expected = LocalTime.of(3, 4, 5);

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.TIME, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(expected, record.extractTimeFrom(0, source));
        Assertions.assertEquals("03:04:05", record.extractStringFrom(0, source));
        Assertions.assertEquals(9, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadDateTime() throws ParseException {
        var record = loadRecordWithValueColumn("DateTime", 19);
        var source = wrap(new byte[]{50, 48, 50, 49, 45, 48, 49, 45, 48, 50, 32, 48, 51, 58, 48, 52, 58, 48, 53, 0});

        var expected = LocalDateTime.of(2021, 1, 2, 3, 4, 5);

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.DATETIME, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals(expected, record.extractDateTimeFrom(0, source));
        Assertions.assertEquals("2021-01-02 03:04:05", record.extractStringFrom(0, source));
        Assertions.assertEquals(20, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadBool() {
        var record = loadRecordWithValueColumn("Bool", 1);
        var source = wrap(new byte[]{1});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.BOOLEAN, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertTrue(record.extractBooleanFrom(0, source));
        Assertions.assertEquals(1, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadByte() {
        var record = loadRecordWithValueColumn("Byte", 2);
        var source = wrap(new byte[]{23, 0});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.BYTE, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertEquals((byte) 23, record.extractByteFrom(0, source));
        Assertions.assertEquals(2, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadBlob() {
        var record = loadRecordWithValueColumn("Blob", 100);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.BLOB, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertArrayEquals(new byte[]{}, record.extractBlobFrom(0, source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadSpatialObj() {
        var record = loadRecordWithValueColumn("SpatialObj", 100);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8});

        Assertions.assertEquals(1, record.fields.length);
        Assertions.assertSame(DataType.BLOB, record.fields[0].dataType());
        Assertions.assertEquals("value", record.fields[0].name());
        Assertions.assertArrayEquals(new byte[]{}, record.extractBlobFrom(0, source));
    }

    private static YxdbRecord loadRecordWithValueColumn(String type, int size) {
        var field = YxdbField.makeField(0, 0, "value", type, "SOURCE", "DESCRIPTION", () -> size, () -> 0);
        var fields = new YxdbField[]{field};
        return new YxdbRecord(fields);
    }

    private static ByteBuffer wrap(byte[] source) {
        return ByteBuffer.wrap(source).order(ByteOrder.LITTLE_ENDIAN);
    }
}
