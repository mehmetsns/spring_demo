package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class MyAppKafka {

    public static final SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();

    public static void readKafkaWriteParquet() {

        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "postEmployee")
                .load();

        Dataset<Row> resultDf;

        df = df.withWatermark("timestamp", "1 minutes")
                .groupBy(
                        window(col("timestamp"), "1 minutes", "1 minutes"),
                        col("value")
                ).agg(count("*").alias("count"));


        resultDf = df.selectExpr("window", "CAST(value AS STRING)", "count");

        StructType schema = new StructType()
                .add("emp_no", IntegerType)
                .add("birth_date", StringType)
                .add("first_name", StringType)
                .add("last_name", StringType)
                .add("gender", StringType)
                .add("hire_date", StringType);

        resultDf = resultDf.select(col("window"), from_json(col("value"), schema).as("data"),
                col("count"))
                .select("window", "data.*", "count");

        resultDf = resultDf.orderBy(desc("count"));
        resultDf.show();
        resultDf.repartition(1).write().parquet("kafkaStream.parquet");

    }

    public static void readKafkaStreamWriteParquet() {

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "postEmployee")
                .load();

        Dataset<Row> resultDf;

        df = df.withWatermark("timestamp", "20 seconds")
                .groupBy(
                        window(col("timestamp"), "1 minutes", "1 minutes"),
                        col("value")
                ).agg(count("*").alias("count"));


        resultDf = df.selectExpr("window", "CAST(value AS STRING)", "count");

        StructType schema = new StructType()
                .add("emp_no", IntegerType)
                .add("birth_date", StringType)
                .add("first_name", StringType)
                .add("last_name", StringType)
                .add("gender", StringType)
                .add("hire_date", StringType);

        resultDf = resultDf.select(col("window"), from_json(col("value"), schema).as("data"),
                col("count"))
                .select("window", "data.*", "count");


        try {
            resultDf.repartition(1).writeStream()
                    .format("parquet")        // can be "orc", "json", "csv", etc.
                    .option("path", "path/to/destination/dir")
                    .outputMode("append")
                    .option("checkpointLocation", "/usr/local/spark/chkpoint/")
                    .queryName("kafkaStream")
                    .start().awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {

        readKafkaWriteParquet();
    }

}
