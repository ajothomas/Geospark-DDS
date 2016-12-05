package org.datasyslab.geospark.hotSpot;

/**
 * Created by ajothomas on 12/2/16.
 */

import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.join.ArrayListBackedIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.datasyslab.geospark.spatialRDD.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Envelope;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.HashMap;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.datasyslab.geospark.spatialRDD.DatePointRDD;
import java.text.DecimalFormat;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Map;
import java.util.Iterator;

public class Trial2 implements Serializable{
    public static Logger logObj = Logger.getLogger(Trial2.class.getName());


    public static int returnAttribute(Envelope env, Map hm){
        if(hm.containsKey(env))
            return (int)hm.get(env);
        else
            return 0;
    }

    public static double returnWeight(Envelope env, Map hm){
        if(hm.containsKey(env))
            return 1.0;
        else
            return 0.0;
    }


    public static double sumFunction(ArrayList<Integer> arr , String operation){
        double sum = 0.0;
        if(operation.toLowerCase().trim().equalsIgnoreCase("normalsum")) {
            for(int element:arr)
                sum+=(double)element;
        }
        else {
            for(int element:arr){
                double sqr = element*element;
                sum += sqr;
            }
        }

        return sum;
    }

    public static ArrayList<Integer> getAttributeNeighbours(Envelope env, Map hm) {
        ArrayList<Integer> attributeValues = new ArrayList<>();
        double x1 = env.getMinX();
        double y1 = env.getMinY();
        attributeValues.add(returnAttribute(env, hm));
        attributeValues.add(returnAttribute(new Envelope(x1-1, x1   , y1-1, y1 ), hm));
        attributeValues.add(returnAttribute(new Envelope(x1  , x1+1 , y1-1, y1 ), hm));
        attributeValues.add(returnAttribute(new Envelope(x1+1, x1+2 , y1-1, y1 ), hm));

        attributeValues.add(returnAttribute(new Envelope(x1-1, x1   , y1, y1+1 ), hm));
        //attributeValues.add((int)returnAttribute(new Envelope(x1, x1+1 , y1, y1+1 ), hm));
        attributeValues.add(returnAttribute(new Envelope(x1+1, x1+2 , y1, y1+1 ), hm));

        attributeValues.add(returnAttribute(new Envelope(x1-1, x1   , y1+1, y1+2 ), hm));
        attributeValues.add(returnAttribute(new Envelope(x1  , x1+1 , y1+1, y1+2 ), hm));
        attributeValues.add(returnAttribute(new Envelope(x1+1, x1+2 , y1+1, y1+2 ), hm));

        return attributeValues;
    }

    public static double getNeighborWeight(Envelope env, Map hm) {
        double totalWeight = 0.0;
        double x1 = env.getMinX();
        double y1 = env.getMinY();
        totalWeight += returnWeight(env, hm);
        totalWeight += returnWeight(new Envelope(x1-1, x1   , y1-1, y1 ), hm);
        totalWeight += returnWeight(new Envelope(x1  , x1+1 , y1-1, y1 ), hm);
        totalWeight += returnWeight(new Envelope(x1+1, x1+2 , y1-1, y1 ), hm);

        totalWeight += returnWeight(new Envelope(x1-1, x1   , y1, y1+1 ), hm);
        //attributeValues.add((int)returnAttribute(new Envelope(x1     , x1+1 , y1, y1+1 ), hm));
        totalWeight += returnWeight(new Envelope(x1+1, x1+2 , y1, y1+1 ), hm);

        totalWeight += returnWeight(new Envelope(x1-1, x1   , y1+1, y1+2 ), hm);
        totalWeight += returnWeight(new Envelope(x1  , x1+1 , y1+1, y1+2 ), hm);
        totalWeight += returnWeight(new Envelope(x1+1, x1+2 , y1+1, y1+2 ), hm);

        return totalWeight;
    }

    /**
     * T
     * @param sc
     * @param DatePointRDDObj
     * @param DateRectangleRDDObj
     * @return
     */
    public static void generateAttribute(JavaSparkContext sc, DatePointRDD DatePointRDDObj, DateRectangleRDD DateRectangleRDDObj, String outputPath){

        JavaRDD<NewPoint> DatePoints = DatePointRDDObj.getRawDatePointRDD();
        JavaRDD<NewEnvelope> DateRectangles = DateRectangleRDDObj.getRawDateRectangleRDD();

        HashMap < Integer, Map<Envelope, Integer>> hm = new HashMap< >();

        /*
        * Calculating attribute value
        * */
        for(int i=1;i<=5;i++) {
            //PointRDD point = DatePoints.filter(s->s.getDateStep()==i);
            final int j = i;

            JavaRDD<NewPoint> DatePoints2  = DatePoints.filter(new Function<NewPoint, Boolean>() {
                @Override
                public Boolean call(NewPoint newPoint) throws Exception {
                    int k=j;
                    return newPoint.getDateStep()==k;
                }
            });

            JavaRDD<Point> DatePoints3 = DatePoints2.map(new Function<NewPoint, Point>() {
                @Override
                public Point call(NewPoint point){
                    return point.getPoint();
                }
            });
            //DatePoints3.cache();
            JavaRDD<NewEnvelope> DateRectangles2  = DateRectangles.filter(new Function<NewEnvelope, Boolean>() {
                @Override
                public Boolean call(NewEnvelope newEnv) throws Exception {
                    int k=j;
                    return newEnv.getDateStep()==k;
                }
            });


            JavaRDD<Envelope> DateRectangles3 = DateRectangles2.map(new Function<NewEnvelope, Envelope>() {
                @Override
                public Envelope call(NewEnvelope env){
                    return env.getEnvelope();
                }
            });
            //DateRectangles3.cache();

            RectangleRDD rectRDD = new RectangleRDD(DateRectangles3);

            // spatial join query
            PointRDD pointRDD = new PointRDD(DatePoints3,"rtree");
            JoinQuery joinQueryObj = new JoinQuery(sc, pointRDD, rectRDD);
            JavaPairRDD<Envelope, HashSet<Point>> spatialJoinQueryOutput =  joinQueryObj.SpatialJoinQuery(pointRDD,rectRDD);

            // collecting the results of the spatial query and converting it into a pair RDD
            JavaPairRDD<Envelope, Integer> counts = spatialJoinQueryOutput.mapToPair(new PairFunction<Tuple2<Envelope,HashSet<Point>>, Envelope, Integer>() {
                @Override
                public Tuple2<Envelope, Integer> call(final Tuple2 t2) {
                    Envelope tempEnv = (Envelope) t2._1;
                    HashSet<Point> tempPoints = (HashSet<Point>) t2._2;
                    return new Tuple2<Envelope, Integer>(tempEnv, tempPoints.size());
                }
            });

            // converting the pair RDD to a java map
            Map<Envelope, Integer> datePlane = counts.collectAsMap();
            hm.put(i, datePlane);
            // not really required
//            JavaRDD<String> AfterAttributeEnvelopeRDD =  spatialJoinQueryOutput.map(new Function<Tuple2<Envelope,HashSet<Point>>, String>() {
//                int k=j;
//                @Override
//                public String call(Tuple2 t2){
//                    Envelope tempEnv = (Envelope) t2._1;
//                    HashSet<Point> tempPoints = (HashSet<Point>) t2._2;
//                    //datePlane.put(tempEnv,tempPoints.size());
//                    return "ENV : "+tempEnv+", SIZE : "+tempPoints.size();
//                }
//            });

            //ArrayList<String> finalOutput1 = new ArrayList< >();
//            Iterator it = datePlane.entrySet().iterator();
//            while (it.hasNext()) {
//                Map.Entry pair = (Map.Entry)it.next();
//                //finalOutput1.add(""+ pair.getValue() + " ## VALUE : "+ Long.toString((long)pair.getValue()));
//                finalOutput1.add(""+ pair.getKey() + " ## VALUE : "+ pair.getValue());
//            }
//            JavaRDD<String> finalOutputRDD1 = sc.parallelize(finalOutput1);
//            finalOutputRDD1.saveAsTextFile(outputPath+"/finalOutput1");

            //ArrayList<String> tempResult = AfterAttributeEnvelopeRDD.collect().toArray();
            //AfterAttributeEnvelopeRDD.saveAsTextFile("hdfs://master:54310/user/hadoop/project/output/temp"+i);
            //logObj.info("################ "+i+" : "+AfterAttributeEnvelopeRDD.count()+" ::: "+datePlane.size());


        }


        /*
        * Calculating the Z-Score
        *
        */

        Map<Double,String> results = new TreeMap<>(Collections.reverseOrder());
        //int N = 70680;
        int N = 11400;
        for(int i=1; i<=5; i++){
            for (double j = 4050; j<=4089; j+=1) {
                for (double k = -7425; k <= -7369; k += 1) {
                    double x1 = j;
                    double y1 = k;
                    double neighbourWeights = 0.0;
                    ArrayList<Integer> attributeValues = new ArrayList< >();

                    Envelope referenceEnv = new Envelope(x1, x1+1, y1, y1+1);

                    Map <Envelope, Integer> referenceEnvMap = hm.get(i);
                    logObj.info("###################"+referenceEnv+", "+referenceEnvMap.size()+","+referenceEnvMap.get(referenceEnv));

                    neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvMap);
                    attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvMap));
                    if(i>1) {
                        Map <Envelope, Integer> referenceEnvM1Map = hm.get(i-1);
                        attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvM1Map));
                        neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvM1Map);

                    }
                    if (i<5) {
                        Map <Envelope, Integer> referenceEnvP1Map = hm.get(i+1);
                        attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvP1Map));
                        neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvP1Map);
                    }

                    double ns = sumFunction(attributeValues, "normalsum");
                    double ss = sumFunction(attributeValues, "squaresum");
                    double XBar = sumFunction(attributeValues, "normalsum")/N;
                    double stdDev = Math.sqrt( (sumFunction(attributeValues, "squaresum")/N) - XBar*XBar );

                    double numerator = sumFunction(attributeValues, "normalsum") - XBar*neighbourWeights;
                    double denominator = stdDev * Math.sqrt((N*neighbourWeights - neighbourWeights*neighbourWeights)/(N-1)) ;
                    //logObj.info("##############"+referenceEnv.getMinX()+", "+referenceEnv.getMinY()+", "+i+", Xbar"+df.format(XBar)+", Neighbor Weights : "+df.format(neighbourWeights));

                    double zscore = 0.0;
                    if(denominator>0.0){
                        zscore = numerator/denominator;
                    }

                    String output = "~~ "+referenceEnv.getMinX()+", "+referenceEnv.getMinY()+", "+i+", "+zscore+", "+ns+", "+ss+", "+neighbourWeights;
                    results.put(zscore,output);

                }
            }
        }

        ArrayList<String> finalOutput = new ArrayList< >();
        Iterator it = results.entrySet().iterator();
        int i=0;
        while (it.hasNext()) {
            if(i++>=50)
                break;
            Map.Entry pair = (Map.Entry)it.next();
            finalOutput.add((String)pair.getValue());
        }
        logObj.info("################ : Result size "+results.size());
        JavaRDD<String> finalOutputRDD = sc.parallelize(finalOutput);
        finalOutputRDD.saveAsTextFile(outputPath+"/finalOutput");

    }

    /**
     * Reading the input point data and transforming it into appropriate format
     * @param sc
     * @param inputPath
     * @param outputPath
     * @return
     */
    public static void formatInputCSV(JavaSparkContext sc, String inputPath, String outputPath) throws IOException{

        logObj.info("################ READING TEXT FILE ################");

//        FileSystem fs =FileSystem.get(new Configuration());
//        conf.set("fs.default.name", "hdfs://master:9000");
//        String outputPath2 = outputPath.substring(StringUtils.ordinalIndexOf(outputPath, "/",3));
//        logObj.info("################################ Deleted : "+outputPath2+"/pointsData");
//        //Path pointsDataFolderPath= new Path(outputPath2+"/pointsData");
//        Path pointsDataFolderPath= new Path("/user/hadoop/project/output/");
//
//        // true means delete recursively
//        if (fs.exists(pointsDataFolderPath)) {
//            fs.delete(pointsDataFolderPath, true);
//            logObj.info("################################ Deleted : "+outputPath2+"/pointsData");
//        }
        final JavaRDD<String> lines = sc.textFile(inputPath);

        // removing noise
        JavaRDD<String> newLines1 = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] lineSplit = s.split(",");
                String longitudeStr = lineSplit[5];
                String latitudeStr = lineSplit[6];
                //40.5N – 40.9N, longitude 73.7W – 74.25W
                if( ( Double.parseDouble(latitudeStr)>=40.5 && Double.parseDouble(latitudeStr)<=40.9 ) &&
                        ( Double.parseDouble(longitudeStr)<=-73.70 && Double.parseDouble(longitudeStr)>=-74.25 ) )
                    return true;
                else
                    return false;
            }
        });

        logObj.info("################ new : "+newLines1.count());
        // filtering the required columns
        JavaRDD<String> newLines2 = newLines1.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] lineSplit = s.split(",");
                //logObj.info("################ : "+lineSplit.length);
                String pickUpTime = lineSplit[1];
                String dateStep = pickUpTime.substring(8,10);

                String longitudeStr = ""+(int)(Math.floor(Double.parseDouble(lineSplit[5])*100));
                String latitudeStr = ""+(int)(Math.floor(Double.parseDouble(lineSplit[6])*100));
                return dateStep+","+latitudeStr+","+longitudeStr;
            }
        });
        newLines2.saveAsTextFile(outputPath+"/pointsData");

        // generating envelopes
        ArrayList <String> DayEnvelopeStr = new ArrayList< >();

        for(int i=1; i<=5; i++){
            for (double j = 4050; j<=4089; j+=1){
                for (double k = -7425; k <= -7369; k += 1) {
                    DayEnvelopeStr.add( ""+i+","+ j +","+ k +","
                            + (j+1) +","+(k+1) );
                }
            }
        }
        JavaRDD<String> rectangleData = sc.parallelize(DayEnvelopeStr);
        rectangleData.saveAsTextFile(outputPath+"/rectangleData");
    }

    public static void main(String args[]) throws IOException{

        String outputPath = args[1];
        String inputPath = args[0];

        // Creating spark context
        SparkConf conf = new SparkConf().setAppName("org.datasyslab.geospark.hotSpot").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // processing base input and creating
        formatInputCSV(sc, inputPath, outputPath);

        DatePointRDD DatePointRDDObj = new DatePointRDD(sc, outputPath+"/pointsData", 0, "csv");
        logObj.info("################ : "+DatePointRDDObj.getRawDatePointRDD().count());

        DateRectangleRDD DateRectangleRDDObj = new DateRectangleRDD(sc, outputPath+"/rectangleData", 0, "csv");
        logObj.info("################ : "+DateRectangleRDDObj.getRawDateRectangleRDD().count());

        generateAttribute(sc, DatePointRDDObj,DateRectangleRDDObj, outputPath);

    }
}