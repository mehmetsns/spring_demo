package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PfOperations {

    public static final SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();



    public Dataset<Row> readTableFromMysql(String dbName, String tableName) {


        return spark.sqlContext()
                .read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/" + dbName)
                .option("user", "root")
                .option("password", "123")
                .option("dbtable", tableName)
                .load();

    }

    public void saveAsParquet(Dataset<Row> table,String path){
        table.write().parquet(path);
    }

    public Dataset<Row> readFromParquet(String path){

        return spark.read().parquet(path);
    }

    public void close(){
        spark.close();
    }
}
