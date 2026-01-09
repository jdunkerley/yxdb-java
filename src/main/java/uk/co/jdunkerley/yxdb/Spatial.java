package uk.co.jdunkerley.yxdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Spatial contains a static function to translate SpatialObj fields into GeoJSON.
 */
public final class Spatial {
    final static private int BytesPerPoint = 16;

    private record Point(double lng, double lat) {}

    private Spatial() {
    }

    /**
     * ToGeoJson translates SpatialObj fields into GeoJSON.
     * <p>
     * Alteryx stores spatial objects in a binary format. This function reads the binary format and converts it to a GeoJSON string.
     *
     * @param value The object read from a SpatialObj field
     * @return A GeoJSON string representing the spatial object
     * @throws IllegalArgumentException The blob is not a valid spatial object
     */
    public static String toGeoJson(byte[] value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }

        if (value.length < 20) {
            for (var b : value) {
                if (b != 0) {
                    throw new IllegalArgumentException("bytes are not a spatial object");
                }
            }
            return "";
        }

        var buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
        var objType = buffer.getInt(0);
        return switch (objType) {
            case 8 -> parsePoints(buffer);
            case 3 -> parseLines(buffer);
            case 5 -> parsePoly(buffer);
            default -> throw new IllegalArgumentException("bytes are not a spatial object");
        };
    }

    private static String parsePoints(ByteBuffer value) {
        var totalPoints = value.getInt(36);
        return totalPoints == 1
                ? geoJson("Point", getCoordAt(value, 40))
                : parseMultiPoint(value);
    }

    private static String parseMultiPoint(ByteBuffer value) {
        var points = new ArrayList<Point>();
        var i = 40;
        while (i < value.capacity()) {
            points.add(getCoordAt(value, i));
            i += BytesPerPoint;
        }
        return geoJson("MultiPoint", points);
    }

    private static String parseLines(ByteBuffer value) {
        var lines = parseMultiPointObjects(value);
        return lines.size() == 1
                ? geoJson("LineString", lines.getFirst())
                : geoJson("MultiLineString", lines);
    }

    private static String parsePoly(ByteBuffer value) {
        var poly = parseMultiPointObjects(value);
        if (poly.size() == 1) {
            return geoJson("Polygon", poly);
        }

        var coordinates = new ArrayList<ArrayList<ArrayList<Point>>>();
        coordinates.add(poly);
        return geoJson("MultiPolygon", coordinates);
    }

    private static ArrayList<ArrayList<Point>> parseMultiPointObjects(ByteBuffer value) {
        var endingIndices = getEndingIndices(value);

        var i = 48 + (endingIndices.length * 4) - 4;
        var objects = new ArrayList<ArrayList<Point>>();
        for (var endingIndex : endingIndices) {
            var line = new ArrayList<Point>();
            while (i < endingIndex) {
                line.add(getCoordAt(value, i));
                i += BytesPerPoint;
            }
            objects.add(line);
        }
        return objects;
    }

    private static int[] getEndingIndices(ByteBuffer value) {
        var totalObjects = value.getInt(36);
        var totalPoints = (int) value.getLong(40);
        var endingIndices = new int[totalObjects];

        var i = 48;
        var startAt = 48 + ((totalObjects - 1) * 4);
        for (var j = 1; j < totalObjects; j++) {
            var endingPoint = value.getInt(i);
            var endingIndex = (endingPoint * BytesPerPoint) + startAt;
            endingIndices[j - 1] = endingIndex;
            i += 4;
        }
        endingIndices[totalObjects - 1] = (totalPoints * BytesPerPoint) + startAt;
        return endingIndices;
    }

    private static Point getCoordAt(ByteBuffer value, int at) {
        var lng = value.getDouble(at);
        var lat = value.getDouble(at + 8);
        return new Point(lng, lat);
    }

    private static String geoJson(String objType, Object coordinates) {
        var builder = new StringBuilder();
        builder.append("{\"yxdbType\":\"");
        builder.append(objType);
        builder.append("\",\"coordinates\":");
        coordinatesToJson(builder, coordinates);
        builder.append('}');
        return builder.toString();
    }

    private static void coordinatesToJson(StringBuilder builder, Object coordinates) {
        switch (coordinates) {
            case List<?> list -> {
                builder.append('[');
                var first = true;
                for (var item : list) {
                    if (!first) {
                        builder.append(',');
                    }
                    coordinatesToJson(builder, item);
                    first = false;
                }

                builder.append(']');
            }
            case Point(double lng, double lat) -> {
                builder.append('[');
                builder.append(lng);
                builder.append(',');
                builder.append(lat);
                builder.append(']');
            }
            default -> builder.append(coordinates);
        }
    }
}
