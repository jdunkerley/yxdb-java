package uk.co.jdunkerley.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YxdbTypeTest {
    @Test
    public void TestBooleanType() {
        var yxdbType = "Bool";
        Assertions.assertEquals(YxdbType.BOOLEAN, yxdbType);
        Assertions.assertEquals(DataType.BOOLEAN, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, -999, 0, null, null);
        Assertions.assertEquals(1, YxdbType.sizeOf(field));
    }

    @Test
    public void TestByteType() {
        var yxdbType = "Byte";
        Assertions.assertEquals(YxdbType.BYTE, yxdbType);
        Assertions.assertEquals(DataType.BYTE, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 1, 0, null, null);
        Assertions.assertEquals(2, YxdbType.sizeOf(field));
    }

    @Test
    public void TestInt16Type() {
        var yxdbType = "Int16";
        Assertions.assertEquals(YxdbType.INT16, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 2, 0, null, null);
        Assertions.assertEquals(3, YxdbType.sizeOf(field));
    }

    @Test
    public void TestInt32Type() {
        var yxdbType = "Int32";
        Assertions.assertEquals(YxdbType.INT32, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 4, 0, null, null);
        Assertions.assertEquals(5, YxdbType.sizeOf(field));
    }

    @Test
    public void TestInt64Type() {
        var yxdbType = "Int64";
        Assertions.assertEquals(YxdbType.INT64, yxdbType);
        Assertions.assertEquals(DataType.LONG, YxdbType.dataTypeOf(yxdbType));

        var field = new YxdbField(-1, -1, null, yxdbType, 8, 0, null, null);
        Assertions.assertEquals(9, YxdbType.sizeOf(field));
    }
}
