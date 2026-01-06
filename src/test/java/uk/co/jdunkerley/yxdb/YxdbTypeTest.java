package uk.co.jdunkerley.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YxdbTypeTest {
    @Test
    public void TestBooleanType() {
        var yxdbType = "Bool";
        Assertions.assertEquals(YxdbType.BOOLEAN, yxdbType);
        Assertions.assertEquals(DataType.BOOLEAN, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(1, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestByteType() {
        var yxdbType = "Byte";
        Assertions.assertEquals(YxdbType.BYTE, yxdbType);
        Assertions.assertEquals(DataType.BYTE, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(2, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestInt16Type() {
        var yxdbType = "Int16";
        Assertions.assertEquals(YxdbType.INT16, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(3, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestInt32Type() {
        var yxdbType = "Int32";
        Assertions.assertEquals(YxdbType.INT32, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(5, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestInt64Type() {
        var yxdbType = "Int64";
        Assertions.assertEquals(YxdbType.INT64, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(9, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestFloatType() {
        var yxdbType = "Float";
        Assertions.assertEquals(YxdbType.FLOAT, yxdbType);
        Assertions.assertEquals(DataType.DOUBLE, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(5, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestDoubleType() {
        var yxdbType = "Double";
        Assertions.assertEquals(YxdbType.DOUBLE, yxdbType);
        Assertions.assertEquals(DataType.DOUBLE, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(9, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestDecimalType() {
        var yxdbType = "FixedDecimal";
        Assertions.assertEquals(YxdbType.DECIMAL, yxdbType);
        Assertions.assertEquals(DataType.DECIMAL, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 13, 2, null, null);
        Assertions.assertEquals(14, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestDateType() {
        var yxdbType = "Date";
        Assertions.assertEquals(YxdbType.DATE, yxdbType);
        Assertions.assertEquals(DataType.DATE, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(11, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestTimeType() {
        var yxdbType = "Time";
        Assertions.assertEquals(YxdbType.TIME, yxdbType);
        Assertions.assertEquals(DataType.TIME, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(9, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestDateTimeType() {
        var yxdbType = "DateTime";
        Assertions.assertEquals(YxdbType.DATETIME, yxdbType);
        Assertions.assertEquals(DataType.DATETIME, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(20, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestStringType() {
        var yxdbType = "String";
        Assertions.assertEquals(YxdbType.STRING, yxdbType);
        Assertions.assertEquals(DataType.STRING, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 15, 0, null, null);
        Assertions.assertEquals(16, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestVStringType() {
        var yxdbType = "V_String";
        Assertions.assertEquals(YxdbType.V_STRING, yxdbType);
        Assertions.assertEquals(DataType.STRING, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(4, YxdbType.sizeOf(field));
        Assertions.assertTrue(field.isVariableLength());
    }

    @Test
    public void TestWStringType() {
        var yxdbType = "WString";
        Assertions.assertEquals(YxdbType.WSTRING, yxdbType);
        Assertions.assertEquals(DataType.STRING, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 15, 0, null, null);
        Assertions.assertEquals(31, YxdbType.sizeOf(field));
        Assertions.assertFalse(field.isVariableLength());
    }

    @Test
    public void TestVWStringType() {
        var yxdbType = "V_WString";
        Assertions.assertEquals(YxdbType.V_WSTRING, yxdbType);
        Assertions.assertEquals(DataType.STRING, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(4, YxdbType.sizeOf(field));
        Assertions.assertTrue(field.isVariableLength());
    }

    @Test
    public void TestBlobType() {
        var yxdbType = "Blob";
        Assertions.assertEquals(YxdbType.BLOB, yxdbType);
        Assertions.assertEquals(DataType.BLOB, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(4, YxdbType.sizeOf(field));
        Assertions.assertTrue(field.isVariableLength());
    }

    @Test
    public void TestSpatialObjType() {
        var yxdbType = "SpatialObj";
        Assertions.assertEquals(YxdbType.SPATIAL_OBJ, yxdbType);
        Assertions.assertEquals(DataType.BLOB, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 0, 0, null, null);
        Assertions.assertEquals(4, YxdbType.sizeOf(field));
        Assertions.assertTrue(field.isVariableLength());
    }
}
