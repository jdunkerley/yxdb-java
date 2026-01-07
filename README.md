## yxdb-java

yxdb-java is a library for reading YXDB files into Java applications.

**Note:** This library only supports reading non-AMP YXDB files and does not support spatial indexes. It cannot write YXDB files.

The library does not have external dependencies and is a pure Java solution.

The public API is contained in the YxdbReader class.

Instantiate YxdbReader using one of the two constructors:
* `new YxdbReader(String)` - load from a file
* `new YxdbReader(InputStream)` - load from an in-memory stream

Iterate through the records in the file using the `next()` method in a while loop:

```
while (reader.next()) {
    // do something
}
```

The reader implements `AutoCloseable` so it can be used in a try-with-resources block:

```
try (YxdbReader reader = new YxdbReader("path/to/file.yxdb")) {
    while (reader.next()) {
        // do something
    }
}
```

Fields can be access via the `readX()` methods on the YxdbReader class. There are readers for each kind of data field supported by YXDB files:

* `readByte(int index)`, `readByte(String name)` - read Byte fields.
* `readBlob(int index)`, `readBlob(String name)` - read Blob and SpatialObj fields.
* `readBoolean(int index)`, `readBoolean(String name)` - read Bool fields.
* `readDate(int index)`, `readDate(String name)` - read Date fields as LocalDate.
* `readTime(int index)`, `readTime(String name)` - read Time fields as LocalTime.
* `readDateTime(int index)`, `readDateTime(String name)` - read DateTime fields as LocalDateTime.
* `readDouble(int index)`, `readDouble(String name)` - read Float, and Double fields.
* `readLong(int index)`, `readLong(String name)` - read Int16, Int32, and Int64 fields.
* `readDecimal(int index)`, `readDecimal(String name)` - read FixedDecimal fields as BigDecimal.
* `readString(int index)`, `readString(String name)` - read String, WString, V_String, and V_WString fields. You can also read Time, Date, DateTime and FixedDecimal fields as strings using this method.

If either the index number or field name is invalid, the read methods will throw an `IllegalArgumentException`.
To read spatial objects, use the `yxdb.Spatial.toGeoJson()` function. The `ToGeoJson()` function translates the binary SpatialObj format into a GeoJSON string.

For convenience, there is also a generic `read(int index)` and `read(String name)` method that returns an `Object`. The returned object will be of the appropriate Java type for the field. Spatial objects will be converted to GeoJSON strings in this method.

### Publishing to Maven Central

To publish a new version of yxdb-java to Maven Central, follow these steps:
1. Update the version number in `build.gradle` to the desired new version.
2. Ensure you have the necessary credentials for Sonatype OSSRH configured in your Gradle properties
  - `user.id`, `user.email`, `user.name`
  - `signing.keyId`, `signing.password`, `signing.secretKeyRingFile`
  - `mavenCentralUsername`, `mavenCentralPassword`
3. Run the Gradle publish task:
   ```
   ./gradlew publishToMavenCentral
   ```

For more details look at https://github.com/vanniktech/gradle-maven-publish-plugin
