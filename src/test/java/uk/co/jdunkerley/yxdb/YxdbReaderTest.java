package uk.co.jdunkerley.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class YxdbReaderTest {
    @Test
    public void TestGetReader() throws IOException {
        var path = "src/test/resources/AllNormalFields.yxdb";
        try (var yxdb = new YxdbReader(path)) {
            Assertions.assertEquals(1, yxdb.numRecords());
            Assertions.assertEquals(16, yxdb.fields().length);

            Assertions.assertTrue(yxdb.next());

            Assertions.assertEquals((byte) 1, yxdb.readByte(0));
            Assertions.assertEquals((byte) 1, yxdb.readByte("ByteField"));
            Assertions.assertTrue(yxdb.readBoolean(1));
            Assertions.assertTrue(yxdb.readBoolean("BoolField"));
            Assertions.assertEquals(16, yxdb.readLong(2));
            Assertions.assertEquals(16, yxdb.readLong("Int16Field"));
            Assertions.assertEquals(32, yxdb.readLong(3));
            Assertions.assertEquals(32, yxdb.readLong("Int32Field"));
            Assertions.assertEquals(64, yxdb.readLong(4));
            Assertions.assertEquals(64, yxdb.readLong("Int64Field"));
            Assertions.assertEquals(new BigDecimal("123.450000"), yxdb.readDecimal(5));
            Assertions.assertEquals(new BigDecimal("123.450000"), yxdb.readDecimal("FixedDecimalField"));
            Assertions.assertEquals(678.9f, yxdb.readDouble(6));
            Assertions.assertEquals(678.9f, yxdb.readDouble("FloatField"));
            Assertions.assertEquals(0.12345, yxdb.readDouble(7));
            Assertions.assertEquals(0.12345, yxdb.readDouble("DoubleField"));
            Assertions.assertEquals("A", yxdb.readString(8));
            Assertions.assertEquals("A", yxdb.readString("StringField"));
            Assertions.assertEquals("AB", yxdb.readString(9));
            Assertions.assertEquals("AB", yxdb.readString("WStringField"));
            Assertions.assertEquals("ABC", yxdb.readString(10));
            Assertions.assertEquals("ABC", yxdb.readString("V_StringShortField"));
            Assertions.assertEquals("B".repeat(500), yxdb.readString(11));
            Assertions.assertEquals("B".repeat(500), yxdb.readString("V_StringLongField"));
            Assertions.assertEquals("XZY", yxdb.readString(12));
            Assertions.assertEquals("XZY", yxdb.readString("V_WStringShortField"));
            Assertions.assertEquals("W".repeat(500), yxdb.readString(13));
            Assertions.assertEquals("W".repeat(500), yxdb.readString("V_WStringLongField"));

            var expectedDate = LocalDate.of(2020, 1, 1);
            Assertions.assertEquals(expectedDate, yxdb.readDate(14));
            Assertions.assertEquals(expectedDate, yxdb.readDate("DateField"));

            var expectedDateTime = LocalDateTime.of(2020, 2, 3, 4, 5, 6);
            Assertions.assertEquals(expectedDateTime, yxdb.readDateTime(15));
            Assertions.assertEquals(expectedDateTime, yxdb.readDateTime("DateTimeField"));

            Assertions.assertFalse(yxdb.next());
        }
    }

    @Test
    public void TestReadObject() throws IOException {
        var path = "src/test/resources/AllNormalFields.yxdb";
        try (var yxdb = new YxdbReader(path)) {
            Assertions.assertEquals(1, yxdb.numRecords());
            Assertions.assertTrue(yxdb.next());

            Assertions.assertEquals((byte) 1, yxdb.read(0));
            Assertions.assertEquals((byte) 1, yxdb.read("ByteField"));
            Assertions.assertEquals(true, yxdb.read(1));
            Assertions.assertEquals(true, yxdb.read("BoolField"));
            Assertions.assertEquals(16L, yxdb.read(2));
            Assertions.assertEquals(16L, yxdb.read("Int16Field"));
            Assertions.assertEquals(32L, yxdb.read(3));
            Assertions.assertEquals(32L, yxdb.read("Int32Field"));
            Assertions.assertEquals(64L, yxdb.read(4));
            Assertions.assertEquals(64L, yxdb.read("Int64Field"));
            Assertions.assertEquals(new BigDecimal("123.450000"), yxdb.read(5));
            Assertions.assertEquals(new BigDecimal("123.450000"), yxdb.read("FixedDecimalField"));
            Assertions.assertEquals((double)678.9f, yxdb.read(6));
            Assertions.assertEquals((double)678.9f, yxdb.read("FloatField"));
            Assertions.assertEquals(0.12345, yxdb.read(7));
            Assertions.assertEquals(0.12345, yxdb.read("DoubleField"));
            Assertions.assertEquals("A", yxdb.read(8));
            Assertions.assertEquals("A", yxdb.read("StringField"));
            Assertions.assertEquals("AB", yxdb.read(9));
            Assertions.assertEquals("AB", yxdb.read("WStringField"));
            Assertions.assertEquals("ABC", yxdb.read(10));
            Assertions.assertEquals("ABC", yxdb.read("V_StringShortField"));
            Assertions.assertEquals("B".repeat(500), yxdb.read(11));
            Assertions.assertEquals("B".repeat(500), yxdb.read("V_StringLongField"));
            Assertions.assertEquals("XZY", yxdb.read(12));
            Assertions.assertEquals("XZY", yxdb.read("V_WStringShortField"));
            Assertions.assertEquals("W".repeat(500), yxdb.read(13));
            Assertions.assertEquals("W".repeat(500), yxdb.read("V_WStringLongField"));

            Assertions.assertFalse(yxdb.next());
        }
    }

    @Test
    public void TestLotsOfRecords() throws IOException {
        var path = "src/test/resources/LotsOfRecords.yxdb";
        try (var yxdb = new YxdbReader(path)) {
            long sum = 0;
            while (yxdb.next()) {
                sum += yxdb.readLong(0);
            }
            Assertions.assertEquals(5000050000L, sum);
        }
    }

    @Test
    public void TestLoadReaderFromStream() throws IOException {
        var stream = new BufferedInputStream(new FileInputStream("src/test/resources/LotsOfRecords.yxdb"));
        try (var yxdb = new YxdbReader(stream)) {
            long sum = 0;
            while (yxdb.next()) {
                sum += yxdb.readLong(0);
            }
            Assertions.assertEquals(5000050000L, sum);
        }
    }

    @Test
    public void RetrievingFieldWithWrongTypeThrows() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/AllNormalFields.yxdb")) {
            yxdb.next();
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBlob(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBoolean(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readByte(1));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDate(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDateTime(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDecimal(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDouble(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readLong(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readString(0));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readTime(0));
        }
    }

    @Test
    public void RetrievingFieldWithInvalidIndexThrows() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/AllNormalFields.yxdb")) {
            yxdb.next();
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.read(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBlob(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBoolean(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readByte(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDate(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDateTime(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDecimal(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDouble(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readLong(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readString(99));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readTime(99));
        }
    }

    @Test
    public void RetrievingFieldWithInvalidNameThrows() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/AllNormalFields.yxdb")) {
            yxdb.next();
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.read("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBlob("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readBoolean("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readByte("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDate("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDateTime("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDecimal("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readDouble("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readLong("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readString("Invalid"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> yxdb.readTime("Invalid"));
        }
    }

    @Test
    public void TestTutorialData() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/TutorialData.yxdb")) {
            var mrCount = 0;
            while (yxdb.next()) {
                if (yxdb.readString("Prefix").equals("Mr")) {
                    mrCount++;
                }
            }
            Assertions.assertEquals(4068, mrCount);
        }
    }

    @Test
    public void TestNewYxdb() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/TestNewYxdb.yxdb")) {
            byte sum = 0;
            while (yxdb.next()) {
                sum += yxdb.readByte(1);
            }
            Assertions.assertEquals(6, sum);
        }
    }

    @Test
    public void TestVeryLongField() throws IOException {
        try (var yxdb = new YxdbReader("src/test/resources/VeryLongField.yxdb")) {
            byte[] blob;

            yxdb.next();
            blob = yxdb.readBlob(1);
            Assertions.assertEquals(604732, blob.length);

            yxdb.next();
            blob = yxdb.readBlob("Blob");
            Assertions.assertNull(blob);

            yxdb.next();
            blob = yxdb.readBlob(1);
            Assertions.assertEquals(604732, blob.length);
        }
    }

    @Test
    public void TestInvalidFile() {
        try (var _ = new YxdbReader("src/test/resources/invalid.txt")) {
            Assertions.fail("Expected exception not thrown");
        } catch (Exception ex) {
            var msg = ex.getMessage();
            Assertions.assertEquals("File is not a valid YXDB file - invalid file type.", msg);
        }
    }

    @Test
    public void TestSmallInvalidFile() {
        try (var _ = new YxdbReader("src/test/resources/invalidSmall.txt")) {
            Assertions.fail("Expected exception not thrown");
        } catch (Exception ex) {
            var msg = ex.getMessage();
            Assertions.assertEquals("File is not a valid YXDB file - invalid header.", msg);
        }
    }

    @Test
    public void TestE2File() {
        try (var _ = new YxdbReader("src/test/resources/ampdata.yxdb")) {
            Assertions.fail("Expected exception not thrown");
        } catch (Exception ex) {
            var msg = ex.getMessage();
            Assertions.assertEquals("Reading AMP YXDB files is not supported.", msg);
        }
    }
}
