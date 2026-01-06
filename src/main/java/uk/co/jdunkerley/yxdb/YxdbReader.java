package uk.co.jdunkerley.yxdb;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * YxdbReader contains the public interface for reading .yxdb files.
 * <p>
 * There are 2 constructors available for YxdbReader. One constructor takes a file path string and another
 * takes an InputStream that reads yxdb-formatted bytes.
 */
public class YxdbReader {
    /**
     * Returns a reader that will parse the .yxdb file specified by the path argument.
     * <p>
     * Iterate through the records in the .yxdb file by calling next().
     * <p>
     * After each call to next(), access the data fields using the readX methods.
     * <p>
     * The reader's stream can be closed early by calling the close() method. If the file is read to the end (i.e. next() returns false), the stream is automatically closed.
     *
     * @param path                      the path to a .yxdb file
     * @throws IllegalArgumentException thrown when the provided file path does not exist or is not a valid YXDB file
     * @throws IOException              thrown when there are issues reading the file
     */
    public YxdbReader(String path) throws IOException, IllegalArgumentException {
        this.path = path;
        var file = new File(this.path);
        stream = new BufferedInputStream(new FileInputStream(file));
        fields = new ArrayList<>();
        loadHeaderAndMetaInfo();
    }

    /**
     * Returns a reader that will parse the .yxdb file contained in the stream.
     * <p>
     * Iterate through the records in the .yxdb file by calling next().
     * <p>
     * After each call to next(), access the data fields using the readX methods.
     * <p>
     * The reader's stream can be closed early by calling the close() method. If the file is read to the end (i.e. next() returns false), the stream is automatically closed.
     *
     * @param stream       an InputStream for a .yxdb-formatted stream of bytes
     * @throws IllegalArgumentException thrown when the stream does not contain a valid YXDB file
     * @throws IOException thrown when there are issues reading the stream
     */
    public YxdbReader(BufferedInputStream stream) throws IOException, IllegalArgumentException {
        path = "";
        this.stream = stream;
        fields = new ArrayList<>();
        loadHeaderAndMetaInfo();
    }

    public long fileID;
    public long creationDate;

    /**
     * The total number of records in the .yxdb file.
     */
    public long numRecords;
    private int metaInfoSize;
    /**
     * Contains the raw XML metadata from the .yxdb file.
     */
    public String metaInfoStr;
    private final List<YxdbField> fields;
    private final BufferedInputStream stream;
    private final String path;
    private YxdbRecord record;
    private BufferedRecordReader recordReader;

    /**
     * @return the list of fields in the .yxdb file. The index of each field in this list matches the index of the field in the .yxdb file.
     */
    public List<YxdbField> listFields() {
        return record.fields;
    }

    /**
     * Closes the stream manually if the reader needs to be ended before reaching the end of the file.
     *
     * @throws IOException thrown when the stream fails to close or closes with an error
     */
    public void close() throws IOException {
        stream.close();
    }

    /**
     * The next function is designed to iterate over each record in the .yxdb file.
     * <p>
     * The standard way of iterating through records is to use a while loop:
     * <p>
     * <code>
     * while (reader.next()) {
     *     // do something
     * }
     * </code>
     *
     * @return             true, if the next record was loaded, and false, if the end of the file was reached
     * @throws IOException thrown when there is an error reading the next record
     */
    public boolean next() throws IOException {
        return recordReader.nextRecord();
    }

    public Object readPolyglotObject(int index) throws IllegalArgumentException {
        var rawObject = readObject(index);
        return switch (rawObject) {
            case null -> null;
            case BigDecimal bd -> bd.toString();
            case LocalDateTime ldt -> ldt.toString();
            default -> rawObject;
        };
    }

    /**
     * Reads a field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the byte field at the specified index. May be null
     * @throws IllegalArgumentException thrown when the index is out of range
     */
    public Object readObject(int index) throws IllegalArgumentException {
        return record.extractObjectFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified byte field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist
     */
    public Object readObject(String name) throws IllegalArgumentException {
        return record.extractObjectFrom(name, recordReader.recordBuffer);
    }


    /**
     * Reads a byte field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the byte field at the specified index. May be null
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a byte field
     */
    public Byte readByte(int index) throws IllegalArgumentException {
        return record.extractByteFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a byte field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified byte field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a byte field
     */
    public Byte readByte(String name) throws IllegalArgumentException {
        return record.extractByteFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the boolean field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a boolean field
     */
    public Boolean readBoolean(int index) throws IllegalArgumentException {
        return record.extractBooleanFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified boolean field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a boolean field
     */
    public Boolean readBoolean(String name) throws IllegalArgumentException {
        return record.extractBooleanFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads an integer field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the long integer field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a long integer field
     */
    public Long readLong(int index) throws IllegalArgumentException {
        return record.extractLongFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads an integer field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified long integer field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a long integer field
     */
    public Long readLong(String name) throws IllegalArgumentException {
        return record.extractLongFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a floating point field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the numeric field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a numeric field
     */
    public Double readDouble(int index) throws IllegalArgumentException {
        return record.extractDoubleFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a floating point field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified numeric field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a numeric field
     */
    public Double readDouble(String name) throws IllegalArgumentException {
        return record.extractDoubleFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a fixed decimal field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the numeric field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a numeric field
     */
    public BigDecimal readDecimal(int index) throws IllegalArgumentException {
        return record.extractDecimalFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a fixed decimal field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified numeric field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a numeric field
     */
    public BigDecimal readDecimal(String name) throws IllegalArgumentException {
        return record.extractDecimalFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the text field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a text field
     */
    public String readString(int index) throws IllegalArgumentException {
        return record.extractStringFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified text field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a text field
     */
    public String readString(String name) throws IllegalArgumentException {
        return record.extractStringFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a date field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the date/datetime field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a date/datetime field
     */
    public LocalDate readDate(int index) throws IllegalArgumentException {
        return record.extractDateFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a date field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified date/datetime field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a date field
     */
    public LocalDate readDate(String name) throws IllegalArgumentException {
        return record.extractDateFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a time field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the date/datetime field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a date/datetime field
     */
    public LocalTime readTime(int index) throws IllegalArgumentException {
        return record.extractTimeFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a time field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified date/datetime field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a date field
     */
    public LocalTime readTime(String name) throws IllegalArgumentException {
        return record.extractTimeFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a datetime field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the date/datetime field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a date/datetime field
     */
    public LocalDateTime readDateTime(int index) throws IllegalArgumentException {
        return record.extractDateTimeFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a datetime field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified date/datetime field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a date field
     */
    public LocalDateTime readDateTime(String name) throws IllegalArgumentException {
        return record.extractDateTimeFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the blob field, as an array of bytes, at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a blob field
     */
    public byte[] readBlob(int index) throws IllegalArgumentException {
        return record.extractBlobFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified blob field, as an array of bytes. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a blob field
     */
    public byte[] readBlob(String name) throws IllegalArgumentException {
        return record.extractBlobFrom(name, recordReader.recordBuffer);
    }

    private void loadHeaderAndMetaInfo() throws IOException, IllegalArgumentException {
        var header = getHeader();

        var fileType = new String(header.array(), 0, 64, StandardCharsets.ISO_8859_1).trim();
        if ("Alteryx e2 Database file".equals(fileType)) {
            throw closeStreamWithException("Reading AMP YXDB files is not supported.");
        }
        if (!fileType.startsWith("Alteryx Database File")) {
            throw closeStreamWithException();
        }

        fileID = header.getLong(64);
        creationDate = header.getLong(72);
        numRecords = header.getLong(104);
        metaInfoSize = header.getInt(80);

        loadMetaInfo();

        record = YxdbRecord.newFromFieldList(fields);
        recordReader = new BufferedRecordReader(stream, record.fixedSize, record.hasVar, numRecords);
    }

    private void loadMetaInfo() throws IOException, IllegalArgumentException {
        var metaInfoBytes = stream.readNBytes((metaInfoSize*2)-2); //YXDB strings are null-terminated, so exclude the last character
        if (metaInfoBytes.length < (metaInfoSize*2)-2) {
            throw closeStreamWithException();
        }

        var skipped = stream.skip(2);
        if (skipped != 2) {
            throw closeStreamWithException();
        }

        metaInfoStr = new String(metaInfoBytes, StandardCharsets.UTF_16LE);
        getFields();
    }

    private ByteBuffer getHeader() throws IOException, IllegalArgumentException {
        var headerBytes = new byte[512];

        var written = stream.readNBytes(headerBytes,0, 512);
        if (written < 512) {
            throw closeStreamWithException();
        }

        return ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void getFields() throws IllegalArgumentException {
        var nodes = getRecordInfoNodes();

        int position = 0;
        for (int i = 0; i < nodes.getLength(); i++) {
            var field = nodes.item(i);
            if (field.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            var newField = parseField(fields.size(), position, field);
            fields.add(newField);
            position += YxdbType.sizeOf(newField);
        }
    }

    private NodeList getRecordInfoNodes() {
        Document doc;
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(metaInfoStr)));
            doc.getDocumentElement().normalize();
        } catch (Exception ex) {
            throw closeStreamWithException();
        }

        var info = doc.getElementsByTagName("RecordInfo").item(0);
        return info.getChildNodes();
    }

    private YxdbField parseField(int index, int position, Node field) {
        var attributes = field.getAttributes();

        var name = attributes.getNamedItem("name");
        var type = attributes.getNamedItem("type");
        if (name == null || type == null) {
            throw closeStreamWithException();
        }

        var nameStr = name.getNodeValue();
        var typeStr = type.getNodeValue();

        var source = attributes.getNamedItem("source");
        var sourceStr = source != null ? source.getNodeValue() : null;
        var description = attributes.getNamedItem("description");
        var descriptionStr = description != null ? description.getNodeValue() : null;

        try {
            return YxdbField.makeField(
                    index,
                    position,
                    nameStr,
                    typeStr,
                    sourceStr,
                    descriptionStr,
                    () -> parseIntFromNode(field, "size", nameStr),
                    () -> parseIntFromNode(field, "scale", nameStr));
        } catch (IllegalArgumentException ex) {
            throw closeStreamWithException(ex.getMessage());
        }
    }

    private int parseIntFromNode(Node node, String attributeName, String fieldName) {
        var attribute = node.getAttributes().getNamedItem(attributeName);
        if (attribute == null) {
            throw new IllegalArgumentException("Field " + fieldName + " is missing required attribute: " + attributeName);
        }

        try {
            return parseInt(attribute.getNodeValue());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Field " + fieldName + " has invalid integer value for attribute: " + attributeName);
        }
    }

    private IllegalArgumentException closeStreamWithException() {
        return closeStreamWithException("File is not a valid YXDB file.");
    }

    private IllegalArgumentException closeStreamWithException(String message) {
        try {
            stream.close();
        }
        catch (Exception _) {
        }

        return new IllegalArgumentException(message);
    }
}
