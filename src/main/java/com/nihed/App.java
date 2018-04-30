package com.nihed;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.IOException;

public class App {


    private static final String SPARK_PARQUET_COMPRESSION_CODEC = "spark.sql.parquet.compression.codec";

    private static final String SPARK_AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec";

    private static final String AVRO_COMPRESSION_CODEC = "avro.output.codec";

    private static final String SHOULD_COMPRESS_OUTPUT = "spark.hadoop.mapred.output.compress";

    private static final String OUTPUT_COMPRESSION_CODEC = "spark.hadoop.mapred.output.compression.codec";



    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("sample data")
                .setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        DataFrame dataFrame = sqlContext.read().json("src/main/resources/data.json");

        FileUtils.deleteDirectory(new File("data"));
        new File("data").mkdirs();

        //write json without compression
        dataFrame.write().json("data/json.nocompression");

        //write parquet with snappy
        sqlContext.setConf(SPARK_PARQUET_COMPRESSION_CODEC, "snappy");
        dataFrame.write().parquet("data/parquet.snappy");

        //write parquet with gzip
        sqlContext.setConf(SPARK_PARQUET_COMPRESSION_CODEC, "gzip");
        dataFrame.write().parquet("data/parquet.gzip");

        //write parquet without compression
        sqlContext.setConf(SPARK_PARQUET_COMPRESSION_CODEC, "uncompressed");
        dataFrame.write().parquet("data/parquet.none");


        //write avro without compression
        sqlContext.setConf(SPARK_AVRO_COMPRESSION_CODEC, "uncompressed");
        dataFrame.write().format("com.databricks.spark.avro").save("data/avro.none");

        //write avro with snappy compression
        sqlContext.setConf(SPARK_AVRO_COMPRESSION_CODEC, "snappy");
        dataFrame.write().format("com.databricks.spark.avro").save("data/avro.snappy");

        //write avro with gzip compression
        sqlContext.setConf(SPARK_AVRO_COMPRESSION_CODEC, "deflate");
        dataFrame.write().format("com.databricks.spark.avro").save("data/avro.deflate");


        System.out.println("Json");
        System.out.println(sqlContext.read().json("data/json.nocompression").count());

        System.out.println("Parquet snappy");
        System.out.println(sqlContext.read().parquet("data/parquet.snappy").count());

        System.out.println("Parquet uncompressed");
        System.out.println(sqlContext.read().parquet("data/parquet.none").count());

        System.out.println("Parquet gzip");
        System.out.println(sqlContext.read().parquet("data/parquet.gzip").count());


        System.out.println("Avro snappy");
        System.out.println(sqlContext.read().format("com.databricks.spark.avro").load("data/avro.snappy").count());

        System.out.println("Avro uncompressed");
        System.out.println(sqlContext.read().format("com.databricks.spark.avro").load("data/avro.none").count());

        System.out.println("Avro deflate");
        System.out.println(sqlContext.read().format("com.databricks.spark.avro").load("data/avro.deflate").count());


    }



}
