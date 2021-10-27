// package com.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.*;

public class App 
{   
    public static void main( String[] args )
    {
        System.out.println("Application started");
        if (args.length < 1) 
        {
            throw new IllegalArgumentException("Csv file path on the hdfs must be passed as argument");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        String hdfsUrl = System.getenv("HDFS_URL");
        String dataUrl = args[0];
        
        String csvFileUrl = hdfsUrl + dataUrl;
        String csvEventsUrl = hdfsUrl + args[1];

        String state = "CA";
        String city = "Los Angeles";
        String start_time = "2015-01-01 07:00:00";
        String end_time = "2021-01-01 07:00:00";

        if (isNullOrEmpty(sparkMasterUrl)) 
        {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        if (isNullOrEmpty(hdfsUrl)) 
        {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        SparkSession spark = SparkSession.builder()
            .appName("Acciedents analysis")
            .master(sparkMasterUrl)
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> dataset = spark.read()
            .option("header", "true")
            .csv(csvFileUrl);

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        System.out.println("stopVsGiveWay");
        stopVsGiveWay(dataset, city, 
            start_time, end_time);

        System.out.println("junctionComparison");
        junctionComparison(dataset, city, 
            start_time, end_time);

        System.out.println("distanceAndDurationStatsForState");
        distanceAndDurationStatsForState(dataset, state, 
            start_time, end_time);
            
        System.out.println("cityWithMaxAccidentsInPeiod");
        cityWithMaxAccidentsInPeiod(dataset, 
            start_time, end_time);

        System.out.println("cityAccidentsPOI accidents");
        cityAccidentsPOI(dataset, 
            start_time, end_time);

        System.out.println("Weather conditions during accidents");
        weatherConditionsDuringAcciedents(dataset, city, 
            start_time, end_time);

        System.out.println("Weather Condition Statistics For Streets");
        weatherConditionStatisticsForStreets(dataset, city, 
            start_time, end_time);

        spark.stop();
        spark.close();
    }

    static void stopVsGiveWay(Dataset<Row> ds, String city, String startTime, String endTime){
        Dataset<Row> ds1 = ds.select(
            ds.col("Give_Way"),
            ds.col("Start_Time"),
            ds.col("End_Time"),
            ds.col("Stop"),
            ds.col("City"));
            
        Dataset<Row> filt = ds1.filter(ds1.col("Start_Time").$greater$eq(startTime)
                .and(ds1.col("End_Time").$less$eq(endTime)))
            .groupBy(ds1.col("City"))
            .agg(
                functions.count(ds1.col("City")),
                functions.sum(functions.when(ds1.col("Give_Way").equalTo(false), 0).when(ds1.col("Give_Way").equalTo(true), 1)).as("Give_Way"),
                functions.sum(functions.when(ds1.col("Stop").equalTo(false), 0).when(ds1.col("Stop").equalTo(true), 1)).as("Stop")
                );
            
        filt.sort(filt.col("count(City)").desc()).show();

        Dataset<Row> diff = filt.select(filt.col("Stop"), filt.col("Give_Way"))
            .withColumn("Difference", filt.col("Give_Way").minus(filt.col("Stop")));

        diff.select(functions.sum(diff.col("Difference")).as("Total Difference")).show();
    }


    static void junctionComparison(Dataset<Row> ds, String city, String startTime, String endTime){
        Dataset<Row> ds1 = ds.select(
            ds.col("Junction"),
            ds.col("Traffic_Signal"),
            ds.col("Start_Time"),
            ds.col("End_Time"),
            ds.col("Roundabout"),
            ds.col("City"));
            
        Dataset<Row> filt = ds1.filter(ds1.col("Start_Time").$greater$eq(startTime)
                .and(ds1.col("End_Time").$less$eq(endTime)))
            .groupBy(ds1.col("City"))
            .agg(
                functions.count(ds1.col("City")),
                functions.sum(functions.when(ds1.col("Junction").equalTo(false), 0)
                                       .when(ds1.col("Junction").equalTo(true), 1)
                            ).as("Junction"),
                functions.sum(functions.when(ds1.col("Roundabout").equalTo(false), 0)
                                       .when(ds1.col("Roundabout").equalTo(true), 1)
                            ).as("Roundabout"),
                functions.sum(functions.when(ds1.col("Traffic_Signal").equalTo(false), 0)
                                       .when(ds1.col("Traffic_Signal").equalTo(true), 1)
                            ).as("Traffic_Signal")
                );
            
        filt.sort(filt.col("count(City)").desc()).show();

        Dataset<Row> diff = filt.select(filt.col("Traffic_Signal"), filt.col("Roundabout"))
            .withColumn("Difference", filt.col("Roundabout").minus(filt.col("Traffic_Signal")));

        diff.select(functions.sum(diff.col("Difference")).as("Total Difference")).show();
    }


    static void distanceAndDurationStatsForState(Dataset<Row> ds, 
        String state, String startTime, String endTime)
    {
        String timeFormat = "yyyy-MM-dd HH:mm:ss";

        Dataset<Row> filt = ds.filter(
            ds.col("State").equalTo(state)
            .and(ds.col("Start_Time").$greater$eq(startTime))
            .and(ds.col("End_Time").$less$eq(endTime))
        ).withColumn("Duration", 
            functions.unix_timestamp(ds.col("End_Time"), timeFormat)
                .minus(functions.unix_timestamp(ds.col("Start_Time"), timeFormat))
        );

        Dataset<Row> new_filt = filt.groupBy(filt.col("City")).agg(
            functions.count(filt.col("City")),
            functions.mean(filt.col("Distance(mi)")),
            functions.stddev(filt.col("Distance(mi)")),
            functions.max(filt.col("Distance(mi)")),
            functions.mean(filt.col("Duration")),
            functions.stddev(filt.col("Duration")),
            functions.max(filt.col("Duration"))
            );

        new_filt.sort(new_filt.col("count(City)").desc()).show(10);
    }

    static void weatherConditionStatisticsForStreets(Dataset<Row> ds, 
        String city, String startTime, String endTime)
    {
        String timeFormat = "yyyy-MM-dd HH:mm:ss";

        Dataset<Row> filt = ds.filter(
            ds.col("City").equalTo(city)
            .and(ds.col("Start_Time").$greater$eq(startTime))
            .and(ds.col("End_Time").$less$eq(endTime))
        ).withColumn("Duration", 
            functions.unix_timestamp(ds.col("End_Time"), timeFormat)
                .minus(functions.unix_timestamp(ds.col("Start_Time"), timeFormat))
        ).groupBy(ds.col("Street")).agg(
            functions.count(ds.col("Street")),
            functions.mean(ds.col("Temperature(F)")),
            functions.stddev(ds.col("Temperature(F)")),
            functions.mean(ds.col("Precipitation(in)")),
            functions.stddev(ds.col("Precipitation(in)"))
        );


        System.out.println("Weather Condition Statistics For Streets - Top streets");
        filt.sort(filt.col("count(Street)").desc()).show(10);
        System.out.println("Weather Condition Statistics For Streets - Bottom streets");
        filt.sort(filt.col("count(Street)")).filter(
            filt.col("count(Street)").$greater(10)
        ).show(10);
    }

    static void weatherConditionsDuringAcciedents(Dataset<Row> ds, String city,
        String startTime, String endTime)
    {
        Dataset<Row> filt = ds.filter(
            ds.col("Weather_Condition").isNotNull()
            .and(ds.col("City").equalTo(city))
            .and(ds.col("Start_Time").$greater$eq(startTime))
            .and(ds.col("End_Time").$less$eq(endTime))
        );

        filt = filt.groupBy(filt.col("Weather_Condition"))
            .agg(
                functions.count(filt.col("Weather_Condition")));

        filt.sort(filt.col("count(Weather_Condition)").desc())
            .show();
    }

    static void cityWithMaxAccidentsInPeiod(Dataset<Row> ds, 
        String startTime, String endTime)
    {
        Dataset<Row> filt = ds.filter(ds.col("Start_Time").$greater$eq(startTime)
                .and(ds.col("End_Time").$less$eq(endTime)))
            .groupBy(ds.col("State"), ds.col("City"))
            .agg(functions.count(ds.col("City")));
            
        filt.sort(filt.col("count(City)")).show(10);
        filt.sort(filt.col("count(City)").desc()).show(10);
    }


    static void cityAccidentsPOI(Dataset<Row> ds, 
        String startTime, String endTime)
    {

        Dataset<Row> ds1 = ds.select(
            ds.col("Bump"),
            ds.col("Crossing"),
            ds.col("Give_Way"),
            ds.col("Junction"),
            ds.col("Start_Time"),
            ds.col("End_Time"),
            ds.col("Amenity"),
            ds.col("Roundabout"),
            ds.col("Stop"),
            ds.col("City"));
            
        Dataset<Row> filt = ds1.filter(ds1.col("Start_Time").$greater$eq(startTime)
                .and(ds1.col("End_Time").$less$eq(endTime)))
            .groupBy(ds1.col("City"))
            .agg(
                functions.count(ds1.col("City")),
                functions.sum(functions.when(ds1.col("Bump").equalTo(false), 0).when(ds1.col("Bump").equalTo(true), 1)).as("Bump"),
                functions.sum(functions.when(ds1.col("Crossing").equalTo(false), 0).when(ds1.col("Crossing").equalTo(true), 1)).as("Crossing"),
                functions.sum(functions.when(ds1.col("Give_Way").equalTo(false), 0).when(ds1.col("Give_Way").equalTo(true), 1)).as("Give_Way"),
                functions.sum(functions.when(ds1.col("Junction").equalTo(false), 0).when(ds1.col("Junction").equalTo(true), 1)).as("Junction"),
                functions.sum(functions.when(ds1.col("Roundabout").equalTo(false), 0).when(ds1.col("Roundabout").equalTo(true), 1)).as("Roundabout"),
                functions.sum(functions.when(ds1.col("Stop").equalTo(false), 0).when(ds1.col("Stop").equalTo(true), 1)).as("Stop")
                );
            
        filt.sort(filt.col("count(City)").desc()).show();
    }
    
    static boolean isNullOrEmpty(String str)
    {
        return str == null || str.isEmpty();
    }
}
