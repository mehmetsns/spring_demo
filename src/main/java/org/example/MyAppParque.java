package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.logging.Logger;


public class MyAppParque {


    private static Logger logger = Logger.getLogger(MyAppParque.class.getName());

    public static void main(String[] args) {


        long start = System.currentTimeMillis();

        PfOperations pfMysql = new PfOperations();

        Dataset<Row> deptManager = pfMysql.readFromParquet("dept_manager.parquet");
        Dataset<Row> salaries = pfMysql.readFromParquet("salaries.parquet");


        Dataset<Row> joinTable;
        joinTable = deptManager.join(salaries).where(deptManager.col("emp_no")
                .equalTo(salaries.col("emp_no")))
                .drop(salaries.col("emp_no"))
                .drop(salaries.col("from_date"))
                .drop(salaries.col("to_date"));

        logger.info("After join dataFrame Schema");
        joinTable.printSchema();

        //table column cast
        joinTable = joinTable.withColumn("salary", joinTable.col("salary").cast(DataTypes.StringType));


        logger.info("After join, salary column cast to String");
        joinTable.printSchema();

        joinTable.write().parquet("managerJoinSalary.parquet");


        //// second way with sql query
        ////-----------------------------------------------------
        if (true) {
            deptManager.createOrReplaceTempView("dept_manager");
            salaries.createOrReplaceTempView("salaries");

            Dataset<Row> joinTable2 = PfOperations.spark.sql("select * from dept_manager d join salaries s on " +
                    "s.emp_no = d.emp_no");

            joinTable2 = joinTable2.drop(joinTable2.col("s.emp_no"))
                    .drop(joinTable2.col("s.from_date"))
                    .drop(joinTable2.col("s.to_date"));

            joinTable2.show();
            joinTable2.write().parquet("managerJoinSalary.parquet");
        }
        ///--------------------------------------------------------------------------------------------


        // some time passes
        long end = System.currentTimeMillis();
        String elapsedTime = "Time: " + Long.toString(end - start);
        logger.info(elapsedTime);

    }

}
