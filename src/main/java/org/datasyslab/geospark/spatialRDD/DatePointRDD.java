package org.datasyslab.geospark.spatialRDD;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.formatMapper.NewPointFormatMapper;
import java.io.Serializable;
import org.datasyslab.geospark.spatialRDD.NewPoint;

/**
 * Created by ajothomas on 12/2/16.
 */
public class DatePointRDD implements Serializable {
    static Logger log = Logger.getLogger(NewPointFormatMapper.class.getName());
    /**
     * The total number of records stored in this RDD
     */
    public long totalNumberOfRecords;

    public JavaRDD<NewPoint> rawDatePointRDD;

    /**
     * Get the raw SpatialRDD
     *
     * @return the raw SpatialRDD
     */
    public JavaRDD<NewPoint> getRawDatePointRDD() {
        return rawDatePointRDD;
    }

    /**
     * Set the raw SpatialRDD.
     *
     * @param rawDatePointRDD One existing SpatialRDD
     */
    public void setRawDatePointRDD(JavaRDD<NewPoint> rawDatePointRDD) {
        this.rawDatePointRDD = rawDatePointRDD;
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     */
    public DatePointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {
        // final Integer offset=Offset;
        System.out.println("Hello world : "+InputLocation);
        this.setRawDatePointRDD(
                spark.textFile(InputLocation).map(new NewPointFormatMapper(Offset, Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
    }
}
