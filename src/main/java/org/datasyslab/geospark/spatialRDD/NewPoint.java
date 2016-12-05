package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.formatMapper.PointFormatMapper;
import java.io.Serializable;

/**
 * Created by ajothomas on 12/2/16.
 */
public class NewPoint implements Serializable{
    Point point;
    int dateStep;
    public NewPoint(){

    }
    public void setPoint(Point point){
        this.point = point;
    }

    public void setDateStep(int dateStep){
        this.dateStep = dateStep;
    }

    public Point getPoint(){
        return point;
    }

    public int getDateStep(){
        return dateStep;
    }

    @Override
    public String toString(){
        return "Date: "+dateStep+", Point: "+point;
    }
}
