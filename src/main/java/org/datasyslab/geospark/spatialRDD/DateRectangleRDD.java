package org.datasyslab.geospark.spatialRDD;

/**
 *
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.commons.lang.IllegalClassException;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.formatMapper.RectangleFormatMapper;
import org.datasyslab.geospark.formatMapper.NewRectangleFormatMapper;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.datasyslab.geospark.utils.RectangleXMaxComparator;
import org.datasyslab.geospark.utils.RectangleXMinComparator;
import org.datasyslab.geospark.utils.RectangleYMaxComparator;
import org.datasyslab.geospark.utils.RectangleYMinComparator;
import org.wololo.jts2geojson.GeoJSONReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;


/**
 * The Class RectangleRDD. It accommodates Rectangle object.
 * @author Arizona State University DataSystems Lab
 *
 */

public class DateRectangleRDD implements Serializable {

    /**
     * The total number of records stored in this RDD
     */
    public long totalNumberOfRecords;

    /**
     * The original Spatial RDD which has not been spatial partitioned and has not index
     */
    public JavaRDD<NewEnvelope> rawDateRectangleRDD;

    /**
     * Initialize one SpatialRDD with one existing SpatialRDD
     * @param rawDateRectangleRDD One existing raw RectangleRDD
     */
    public DateRectangleRDD(JavaRDD<NewEnvelope> rawDateRectangleRDD)
    {
        this.setRawDateRectangleRDD(rawDateRectangleRDD);//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     */
    public DateRectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
    {
        //final Integer offset=Offset;
        this.setRawDateRectangleRDD(spark.textFile(InputLocation).map(new NewRectangleFormatMapper(Offset,Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
    }

    /**
     * Get the raw SpatialRDD
     *
     * @return the raw SpatialRDD
     */
    public JavaRDD<NewEnvelope> getRawDateRectangleRDD() {
        return rawDateRectangleRDD;
    }

    /**
     * Set the raw SpatialRDD.
     *
     * @param rawDateRectangleRDD One existing SpatialRDD
     */
    public void setRawDateRectangleRDD(JavaRDD<NewEnvelope> rawDateRectangleRDD) {
        this.rawDateRectangleRDD = rawDateRectangleRDD;
    }

}