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
public class Approach4 implements Serializable{
    public static Logger logObj = Logger.getLogger(Trial2.class.getName());
    public static int startDays = 1;
    public static int totalDays = 31;
    public static int N = 70680;

    public static int returnAttribute(int timeStep, int x1, int y1, Map<String,Integer> Grid3D){
        //CHecking stuff
        String check = "13"+","+"4074"+","+"-7399";
        if(Grid3D.containsKey(check)) {
            int checkValue = (int)Grid3D.get(check);
            //logObj.info("################ RINTING THE COUNT OF POINTS AT ################"+checkValue);
        }

        String key = timeStep+","+x1+","+y1;
        if(Grid3D.containsKey(key)){
            int checkValue = (int)Grid3D.get(key);
            //logObj.info("################ RINTING THE COUNT OF POINTS AT ################"+ checkValue);
            return Grid3D.get(key);
        }
        else{
            return 0;
        }
    }
    // for (int j = 4050; j<=4089; j+=1) {
    //for (int k = -7425; k <= -7369; k += 1) {
    public static int returnWeight(int timeStep, int x1, int y1, Map<String,Integer> Grid3D){
        if(timeStep==startDays-1 || timeStep == totalDays+1 || x1==4049 || x1==4090 || y1==-7426 || y1==-7368)
            return 0;
        else
            return 1;
    }


    public static int sumFunction(ArrayList<Integer> arr , String operation){
        int sum = 0;
        if(operation.toLowerCase().trim().equalsIgnoreCase("normalsum")) {
            for(int element:arr)
                sum+=element;
        }
        else {
            for(int element:arr){
                int sqr = (element*element);
                sum += sqr;
            }
        }

        return sum;
    }

    public static ArrayList<Integer> getAttributeNeighbours(Map<String,Integer> Grid3D, int x1, int y1, int timeStep) {
        ArrayList<Integer> attributeValues = new ArrayList<>();

        if(timeStep>=startDays && timeStep<=totalDays){
            attributeValues.add(returnAttribute(timeStep, x1-1, y1-1, Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1  , y1-1, Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1+1, y1-1, Grid3D));

            attributeValues.add(returnAttribute(timeStep, x1-1, y1  , Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1  , y1  , Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1+1, y1  , Grid3D));

            attributeValues.add(returnAttribute(timeStep, x1-1, y1+1, Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1  , y1+1, Grid3D));
            attributeValues.add(returnAttribute(timeStep, x1+1, y1+1, Grid3D));
        }

        return attributeValues;
    }


    public static double roundToTwo(double value) {
        return (double)Math.round(value * 100d) / 100d;
    }

    public static int getNeighborWeight(Map<String,Integer> Grid3D, int x1, int y1, int timeStep) {
        int totalWeight = 0;

        if(timeStep>=startDays && timeStep<=totalDays){
            totalWeight += returnWeight(timeStep, x1-1, y1-1, Grid3D);
            totalWeight += returnWeight(timeStep, x1  , y1-1, Grid3D);
            totalWeight += returnWeight(timeStep, x1+1, y1-1, Grid3D);

            totalWeight += returnWeight(timeStep, x1-1, y1  , Grid3D);
            totalWeight += returnWeight(timeStep, x1  , y1  , Grid3D);
            totalWeight += returnWeight(timeStep, x1+1, y1  , Grid3D);

            totalWeight += returnWeight(timeStep, x1-1, y1+1, Grid3D);
            totalWeight += returnWeight(timeStep, x1  , y1+1, Grid3D);
            totalWeight += returnWeight(timeStep, x1+1, y1+1, Grid3D);
        }

        return totalWeight;
    }

    public static void formatInputCSV(JavaSparkContext sc, String inputPath, String outputPath) throws IOException{

        logObj.info("################ READING new TEXT FILE ################");
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

        //logObj.info("################ new : "+newLines1.count());
        // filtering the required columns
        JavaRDD<String> newLines2 = newLines1.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] lineSplit = s.split(",");
                //logObj.info("################ : "+lineSplit.length);
                String pickUpTime = lineSplit[1];
                String temp[] = {"40.6931495666503",   "40.6928062438964",    "40.6951026916503",    "40.6945495605468",    "40.6937294006347",    "40.695514678955", "40.6922760009765",    "40.6937141418457",    "40.6930122375488",    "40.6924247741699",    "40.6961479187011",    "40.6962356567382",    "40.6918029785156",    "40.6992988586425",    "40.6926345825195",    "40.6952514648437",    "40.696189880371", "40.6921615600585",    "40.6920928955078",    "40.6949195861816",    "40.6965217590332",    "40.6927909851074",    "40.6925659179687"};
                String temp1[] = {"-73.9995498657226", "-73.9925842285156",   "-73.9919662475585",   "-73.9922637939453",   "-73.9965515136718",   "-73.991943359375",    "-73.9933700561523",   "-73.9927444458007",   "-73.9930343627929",   "-73.9972381591796",   "-73.9964523315429",   "-73.9913024902343",   "-73.9999847412109",   "-73.9912643432617",   "-73.9981155395507",   "-73.9919052124023",   "-73.9913482666015",   "-73.9973678588867",   "-73.9934616088867",   "-73.9921722412109",   "-73.9953689575195",   "-73.9931106567382",   "-73.9910507202148"};
                String templong = lineSplit[5];
                String tempLat = lineSplit[6];
                String latitudeStr1 = "";
                String longitudeStr1 = "";
                for(int i=0;i<temp.length;i++){
                    if(tempLat.equals(temp[i])){
                        latitudeStr1 = ""+(int)((Double.parseDouble(lineSplit[6]))*100);
                        String a = lineSplit[6];

                        //logObj.info("################ --- Data found : "+latitudeStr1);
                        //System.out.println("Data found:" + latitudeStr1);
                    }

                }
                for(int i=0;i<temp1.length;i++){
                    if(templong.equals(temp1[i])){
                        //logObj.info("##"+lineSplit[5] + "##");
                        String a = lineSplit[5];
                        longitudeStr1 = ""+(int)(((Double.parseDouble(lineSplit[5]))*100));
                        //logObj.info("################ Longitue Data found : "+longitudeStr1);
                        //System.out.println(" $$$$$$$$$$$$ Data found:" + longitudeStr1);

                        longitudeStr1 = ""+(int)((Double.parseDouble(a.substring(0,6)))*100);
                        //logObj.info("################ ALtered Data printing : "+longitudeStr1);
                    }

                }

                String dateStep = Integer.toString(Integer.parseInt(pickUpTime.substring(8,10)));
                String longitudeStr = ""+(int)(((Double.parseDouble(lineSplit[5])*100)));
                String latitudeStr = ""+(int)(Double.parseDouble(lineSplit[6])*100);
                /*if(latitudeStr.equals(latitudeStr1) && longitudeStr.equals(longitudeStr1)){
                    logObj.info("################ Macthed Data found : "+longitudeStr1);
                }*/
                return dateStep+","+latitudeStr+","+longitudeStr;
            }
        });
        newLines2.saveAsTextFile(outputPath+"/pointsData");

    }

    /**
     * T
     * @param sc
     * @return
     */
    public static void calculateZScore(JavaSparkContext sc, String outputPath){

        // Reading the point data
        JavaPairRDD<String, Integer> allDatePoint = sc.textFile(outputPath+"/pointsData").mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String x){
                        return new Tuple2(x, 1);
                    }})
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer x, Integer y) {
                        return x + y;
                    }
                });

        double mean=0;
        double std = 0;
        Map<String, Integer> Grid3D = allDatePoint.collectAsMap();
        Iterator it1 = Grid3D.entrySet().iterator();
        while (it1.hasNext()) {
            Map.Entry<String,Integer> pair = (Map.Entry)it1.next();
            int num = pair.getValue();
            mean+= num;
            std += (num*num);
            //logObj.info("################ : PLACE 2 TOTAL COUNT IS "+count);
        }
        String check = "13"+","+"4074"+","+"-7399";
        if(Grid3D.containsKey(check)) {
            int checkValue = (int)Grid3D.get(check);
            //logObj.info("################ AFTER BUILDING THE  GRID RINTING THE COUNT OF POINTS AT ################"+checkValue);
        }

        double meanValue = mean/N;
        double stdValue = Math.sqrt((std/N)  - (meanValue*meanValue));
        logObj.info("################ : Mean VAlue "+meanValue);
        logObj.info("################ : Std  IS "+stdValue);
        //if((int)pair.getValue()>3)
        //logObj.info("################ : TOTAL COUNT IS "+count);


        // final results map
        Map<Double,String> results = new TreeMap<>(Collections.reverseOrder());


        for(int i=startDays; i<=totalDays; i++) {
            for (int x = 4050; x<=4089; x+=1) {
                for (int y = -7425; y <= -7369; y += 1) {

                    ArrayList<Integer> attributeValues = new ArrayList< >();
                    int neighbourWeights = 0;


                    attributeValues.addAll(getAttributeNeighbours(Grid3D, x,y,i));
                    attributeValues.addAll(getAttributeNeighbours(Grid3D, x,y,i+1));
                    attributeValues.addAll(getAttributeNeighbours(Grid3D, x,y,i-1));
                    //System.out.println("Arraylist size###################" + attributeValues.size());
                    neighbourWeights += getNeighborWeight(Grid3D, x,y,i);
                    neighbourWeights += getNeighborWeight(Grid3D, x,y,i+1);
                    neighbourWeights += getNeighborWeight(Grid3D, x,y,i-1);

                    int ns = sumFunction(attributeValues, "normalsum");
                    int ss = sumFunction(attributeValues, "squaresum");


                    double numerator = sumFunction(attributeValues, "normalsum") - meanValue*neighbourWeights;
                    double denominator = stdValue * Math.sqrt((N*neighbourWeights - neighbourWeights*neighbourWeights)/(N-1)) ;
                    //logObj.info("##############"+referenceEnv.getMinX()+", "+referenceEnv.getMinY()+", "+i+", Xbar"+df.format(XBar)+", Neighbor Weights : "+df.format(neighbourWeights));

                    double zscore = 0.0;
                    if(denominator>0.0){
                        zscore = numerator/denominator;
                    }

                    String output = "~~ "+x+", "+y+", "+i+", "+zscore+", "+ns+", "+ss+", "+neighbourWeights;
                    results.put(zscore,output);
                }
            }
        }

        ArrayList<String> finalOutput = new ArrayList< >();
        Iterator it = results.entrySet().iterator();
        int i=0;
        while (it.hasNext()) {
            //if(i++>=50)
            //    break;
            Map.Entry pair = (Map.Entry)it.next();
            finalOutput.add((String)pair.getValue());
        }
        //logObj.info("################ : Result size "+results.size());
        JavaRDD<String> finalOutputRDD = sc.parallelize(finalOutput);
        finalOutputRDD.saveAsTextFile(outputPath+"/finalOutput");
    }
    /*public static void checkNumber(){

        String temp[] = {"40.6931495666503",   "40.6928062438964",    "40.6951026916503",    "40.6945495605468",    "40.6937294006347",    "40.695514678955", "40.6922760009765",    "40.6937141418457",    "40.6930122375488",    "40.6924247741699",    "40.6961479187011",    "40.6962356567382",    "40.6918029785156",    "40.6992988586425",    "40.6926345825195",    "40.6952514648437",    "40.696189880371", "40.6921615600585",    "40.6920928955078",    "40.6949195861816",    "40.6965217590332",    "40.6927909851074",    "40.6925659179687"};
        String temp1[] = {"-73.9995498657226", "-73.9925842285156",   "-73.9919662475585",   "-73.9922637939453",   "-73.9965515136718",   "-73.991943359375",    "-73.9933700561523",   "-73.9927444458007",   "-73.9930343627929",   "-73.9972381591796",   "-73.9964523315429",   "-73.9913024902343",   "-73.9999847412109",   "-73.9912643432617",   "-73.9981155395507",   "-73.9919052124023",   "-73.9913482666015",   "-73.9973678588867",   "-73.9934616088867",   "-73.9921722412109",   "-73.9953689575195",   "-73.9931106567382",   "-73.9910507202148"};
        for(int i=0;i<temp.length;i++) {
            String check = ""+(int)((roundToTwo(Double.parseDouble(temp[i]) * 100)));
            System.out.println(check);
        }
        System.out.println("##############################PRINTING LONGNTIDUDE");
        for(int i=0;i<temp1.length;i++) {
            String check = ""+(int)((roundToTwo(Double.parseDouble(temp1[i]) * 100)));
            System.out.println(check);
        }


    }*/

    public static void main(String args[]) throws IOException {
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$New code");
        String outputPath = args[1];
        String inputPath = args[0];

        // Creating spark context
        SparkConf conf = new SparkConf().setAppName("org.datasyslab.geospark.hotSpot").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // processing base input and creating
        formatInputCSV(sc, inputPath, outputPath);
        //       checkNumber();

        //DatePointRDD DatePointRDDObj = new DatePointRDD(sc, outputPath+"/pointsData", 0, "csv");
        //logObj.info("################ : "+DatePointRDDObj.getRawDatePointRDD().count());

//        DateRectangleRDD DateRectangleRDDObj = new DateRectangleRDD(sc, outputPath+"/rectangleData", 0, "csv");
//        logObj.info("################ : "+DateRectangleRDDObj.getRawDateRectangleRDD().count());

        calculateZScore(sc, outputPath);
    }
}