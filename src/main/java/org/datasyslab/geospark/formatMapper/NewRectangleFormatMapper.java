package org.datasyslab.geospark.formatMapper;


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.util.SystemClock;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.datasyslab.geospark.spatialRDD.NewEnvelope;

/**
 * Created by ajothomas on 12/2/16.
 */
public class NewRectangleFormatMapper implements Serializable,Function<String,NewEnvelope>
{
    Integer offset = 0;
    String splitter = "csv";

    public NewRectangleFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public NewRectangleFormatMapper( String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public NewEnvelope call(String line) throws Exception {
        NewEnvelope newenvelope = new NewEnvelope();
        Envelope rectangle = null;
        List<String> lineSplitList;
        Double x1,x2,y1,y2;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));

                x1 = Double.parseDouble(lineSplitList.get(1));
                y1 = Double.parseDouble(lineSplitList.get(2));
                x2 = Double.parseDouble(lineSplitList.get(3));
                y2 = Double.parseDouble(lineSplitList.get(4));
                rectangle = new Envelope(x1, x2, y1, y2);
                rectangle.setUserData(line);

                newenvelope.setEnvelope(rectangle);
                newenvelope.setDateStep(Integer.parseInt(lineSplitList.get(0)));
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return newenvelope;
    }
}