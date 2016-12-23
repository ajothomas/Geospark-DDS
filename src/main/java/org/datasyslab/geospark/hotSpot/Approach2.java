package org.datasyslab.geospark.hotSpot;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.spatialRDD.DatePointRDD;
import org.datasyslab.geospark.spatialRDD.DateRectangleRDD;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
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
import java.util.List;

/**
 * Created by ajothomas on 12/4/16.
 */
public class Approach2 implements Serializable{
    public static Logger logObj = Logger.getLogger(Trial2.class.getName());
    public static int startDays = 1;
    public static int totalDays = 5;

    public static double returnAttribute(Envelope env, Map<Envelope,Double> hm){
        if(hm.containsKey(env)){
            logObj.info("############################# "+env+" : "+hm.get(env));
            return hm.get(env);
        }
        else{
            logObj.info("############################# "+env+" : "+0.0);
            return 0.0;
        }
    }

    public static double returnWeight(Envelope env, Map<Envelope,Double> hm){
        if(hm.containsKey(env))
            return 1.0;
        else
            return 0.0;
    }


    public static double sumFunction(ArrayList<Double> arr , String operation){
        double sum = 0.0;
        if(operation.toLowerCase().trim().equalsIgnoreCase("normalsum")) {
            for(double element:arr)
                sum+=element;
        }
        else {
            for(double element:arr){
                double sqr = (element*element);
                sum += sqr;
            }
        }

        return sum;
    }

    public static ArrayList<Double> getAttributeNeighbours(Envelope env, Map<Envelope,Double> hm) {
        ArrayList<Double> attributeValues = new ArrayList<>();
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
        double s=0.0;
        for(double att:attributeValues){
            s+=att;
        }

        logObj.info("############################# SUM: "+s);
        return attributeValues;
    }

    public static double getNeighborWeight(Envelope env, Map<Envelope,Double> hm) {
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

    public static void formatInputCSV(JavaSparkContext sc, String inputPath, String outputPath) throws IOException{

        logObj.info("################ READING TEXT FILE ################");
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

    }

    /**
     * T
     * @param sc
     * @param DatePointRDDObj
     * @return
     */
    public static void calculateZScore(JavaSparkContext sc, DatePointRDD DatePointRDDObj, String outputPath){
        JavaRDD<NewPoint> DatePoints = DatePointRDDObj.getRawDatePointRDD();
        //JavaRDD<NewEnvelope> DateRectangles = DateRectangleRDDObj.getRawDateRectangleRDD();

        HashMap < Integer, Map<Envelope, Double>> Grid3D = new HashMap< >();

        for(int i=startDays; i<=totalDays; i++){
            Map<Envelope, Double> Grid2D = new HashMap<>( );
            for (double j = 4050; j<=4089; j+=1){
                for (double k = -7425; k <= -7369; k += 1) {
                    double x1 = j;
                    double y1 = k;
                    Envelope env = new Envelope(x1, x1+1, y1, y1+1);
                    Grid2D.put(env,0.0);
                }
            }
            Grid3D.put(i,Grid2D);
        }

        double sum = 0;
        for(int i=startDays;i<=totalDays;i++) {
            //PointRDD point = DatePoints.filter(s->s.getDateStep()==i);
            final int j = i;
            Map<Envelope, Double> Grid2D = Grid3D.get(i);

            JavaRDD<NewPoint> DatePoints2 = DatePoints.filter(new Function<NewPoint, Boolean>() {
                @Override
                public Boolean call(NewPoint newPoint) throws Exception {
                    int k = j;
                    return newPoint.getDateStep() == k;
                }
            });

            JavaRDD<Point> DatePoints3 = DatePoints2.map(new Function<NewPoint, Point>() {
                @Override
                public Point call(NewPoint point) {
                    return point.getPoint();
                }
            });

            sum = sum+ (int)DatePoints3.count();

            List<Point> pointsAtTime = DatePoints3.collect();
            for(Point eachPoint:pointsAtTime){
                double x1 = eachPoint.getX();
                double y1 = eachPoint.getY();
                Envelope env = new Envelope(x1, x1+1, y1, y1+1);
                if(Grid2D.containsKey(env)){
                    double val = Grid2D.get(env)+1.0;
                    Grid2D.put(env,val);

                    //sum += (long)Grid2D.get(env);
                }
            }
            Grid3D.put(i,Grid2D);
        }

        /*
        Map<Envelope, Integer> Grid2D = Grid3D.get(2);
        Envelope checkEnv = new Envelope((double)4064, (double)4065, (double) -7388,(double) -7387);
        logObj.info("################ : Check env val : "+checkEnv+" : "+(int)Grid2D.get(checkEnv));

        Map<Double,String> results = new TreeMap<>(Collections.reverseOrder());
        //int N = 70680;
        int N = 11400;
            int i=2;
            double x1 = checkEnv.getMinX();
            double y1 = checkEnv.getMinY();
            double neighbourWeights = 0.0;
            ArrayList<Integer> attributeValues = new ArrayList< >();

            Envelope referenceEnv = new Envelope(x1, x1+1, y1, y1+1);

            Map <Envelope, Integer> referenceEnvMap = Grid3D.get(i);
            //logObj.info("###################"+referenceEnv+", "+referenceEnvMap.size()+","+referenceEnvMap.get(referenceEnv));

            neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvMap);
            attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvMap));

            if(i>1) {
                Map <Envelope, Integer> referenceEnvM1Map = Grid3D.get(i-1);
                attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvM1Map));
                neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvM1Map);

            }
            if (i<totalDays) {
                Map <Envelope, Integer> referenceEnvP1Map = Grid3D.get(i+1);
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

        */

        double sum1 = 0;
        for(int i=startDays; i<=totalDays; i++) {
            Map<Envelope, Double> Grid2D = Grid3D.get(i);
            for (double j = 4050; j<=4089; j+=1) {
                for (double k = -7425; k <= -7369; k += 1) {
                    double x1 = j;
                    double y1 = k;
                    Envelope referenceEnv = new Envelope(x1, x1+1, y1, y1+1);
                    sum1 = sum1 + Grid2D.get(referenceEnv);
                }
            }
        }


        /*
        Calculating the Z-Score
        */
        Map<Double,String> results = new TreeMap<>(Collections.reverseOrder());
        int N = 70680;
        //int N = 13680;
        //int N = 11400;
        double sum2 = 0.0;
        for(int i=startDays; i<=totalDays; i++) {
            for (double j = 4050; j<=4089; j+=1) {
                for (double k = -7425; k <= -7369; k += 1) {
                    double x1 = j;
                    double y1 = k;
                    double neighbourWeights = 0.0;
                    ArrayList<Double> attributeValues = new ArrayList< >();

                    Envelope referenceEnv = new Envelope(x1, x1+1, y1, y1+1);

                    Map <Envelope, Double> referenceEnvMap = Grid3D.get(i);
                    //logObj.info("###################"+referenceEnv+", "+referenceEnvMap.size()+","+referenceEnvMap.get(referenceEnv));

                    sum2 = sum2 + returnAttribute(referenceEnv, referenceEnvMap);
                    neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvMap);
                    attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvMap));

                    if(i>startDays) {
                        Map <Envelope, Double> referenceEnvM1Map = Grid3D.get(i-1);
                        attributeValues.addAll(getAttributeNeighbours(referenceEnv, referenceEnvM1Map));
                        neighbourWeights += getNeighborWeight(referenceEnv, referenceEnvM1Map);

                    }
                    if (i<totalDays) {
                        Map <Envelope, Double> referenceEnvP1Map = Grid3D.get(i+1);
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


        logObj.info("################### SUM 01 : " + sum1);
        logObj.info("################### SUM 02 : " + sum);
        logObj.info("################### SUM 03 : " + sum2);

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

    public static void main(String args[]) throws IOException {
        String outputPath = args[1];
        String inputPath = args[0];

        // Creating spark context
        SparkConf conf = new SparkConf().setAppName("org.datasyslab.geospark.hotSpot").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // processing base input and creating
        formatInputCSV(sc, inputPath, outputPath);

        DatePointRDD DatePointRDDObj = new DatePointRDD(sc, outputPath+"/pointsData", 0, "csv");
        //logObj.info("################ : "+DatePointRDDObj.getRawDatePointRDD().count());

//        DateRectangleRDD DateRectangleRDDObj = new DateRectangleRDD(sc, outputPath+"/rectangleData", 0, "csv");
//        logObj.info("################ : "+DateRectangleRDDObj.getRawDateRectangleRDD().count());

        calculateZScore(sc, DatePointRDDObj, outputPath);
    }
}
