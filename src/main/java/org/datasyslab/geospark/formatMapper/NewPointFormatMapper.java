package org.datasyslab.geospark.formatMapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.spatialRDD.NewPoint;
/**
 * Created by ajothomas on 12/2/16.
 */

public class NewPointFormatMapper implements Serializable, Function<String, NewPoint> {
    Integer offset = 0;
    String splitter = "csv";

    public NewPointFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public NewPointFormatMapper( String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public NewPoint call(String line) throws Exception {
        NewPoint newpoint = new NewPoint();
        Point point = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(1)), Double.parseDouble(lineSplitList.get(2)));
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                newpoint.setPoint(point);
                newpoint.setDateStep(Integer.parseInt(lineSplitList.get(0)));
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return newpoint;
    }
}
