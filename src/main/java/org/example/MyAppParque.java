package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MyAppParque {

    public static void main(String[] args) {

        PfOperations pfMysql = new PfOperations();

        String dbName = "employees";
        Dataset<Row> employee = pfMysql.readTableFromMysql(dbName, "dept_manager");
        employee.createOrReplaceTempView("dept_manager");

        Dataset<Row> salaries = pfMysql.readTableFromMysql(dbName, "salaries");
        salaries.createOrReplaceTempView("salaries");

        Dataset<Row> titles = pfMysql.readTableFromMysql(dbName, "titles");
        titles.createOrReplaceTempView("titles");

        String query = "SELECT t.title, s.salary,s.from_date,s.to_date\n" +
                "        FROM dept_manager d join titles t on d.emp_no=t.emp_no\n" +
                "        join salaries s on s.emp_no=d.emp_no\n" +
                "        where t.title !='Manager' " +
                "        order by s.salary desc";
        Dataset<Row> table = PfOperations.spark.sql(query);

        //save table as parquet
        pfMysql.saveAsParquet(table, "managerWithSalaryHistory.parquet");

        // read from parquet file
        //Dataset<Row> table2 = pfMysql.readFromParquet("managerWithSalaryHistory.parquet");
        //table2.show();
        pfMysql.close();

    }

}
