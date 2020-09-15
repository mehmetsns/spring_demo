package org.example.controller;

import org.apache.spark.sql.SparkSession;
import org.example.model.RegisterEmployee;
import org.example.model.SearchEmployee;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


@Controller
public class EmployeeController {

    Connection conn = null;
    boolean isConnect = false;

    SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();



    public String getPsw(int psw) {

        psw = psw % 124;
        return Integer.toString(psw);
    }

    public void connectDatabase() {

        String connectionUrl = "jdbc:mysql://localhost:3306/employees";
        String user = "root";
        String psw = getPsw(123);
        try {
            conn = DriverManager.getConnection(connectionUrl, user, psw);
            isConnect = true;
        } catch (SQLException throwables) {
            isConnect = false;
        }

    }


    @GetMapping(value = "/employee/search")

    @ResponseBody()
    public SearchEmployee searchEmployee(@RequestBody SearchEmployee employee) {

        if (!isConnect)
            connectDatabase();

        employee.search(conn);

        return employee;
    }


    @PostMapping(value = "/employee/register")

    @ResponseBody()
    public String registerEmployee(@RequestBody RegisterEmployee employee) {

        if (!isConnect)
            connectDatabase();

        employee.register(conn);

        return employee.alertMessage();
    }


    @GetMapping(value = "/employee/search/spark")

    @ResponseBody()
    public SearchEmployee searchEmployeeSpark(@RequestBody SearchEmployee employee) {

        employee.searchWithSpark(spark);

        return employee;
    }

    @PostMapping(value = "/employee/register/spark")

    @ResponseBody()
    public String registerEmployeeSpark(@RequestBody RegisterEmployee employee) {

        spark.sparkContext().setLogLevel("ERROR");

        employee.sendKafka(spark);

        return employee.alertMessage();
    }

}


