package org.example.controller;


import jdk.nashorn.internal.objects.annotations.Constructor;
import org.apache.spark.sql.SparkSession;
import org.example.model.RegisterEmployee;
import org.example.model.SearchEmployee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


@Controller
public class EmployeeController {


    Connection conn;
    boolean isConnect = false;
    SparkSession spark;

    String connectionUrl;
    String user;
    String psw;

    @Autowired
    private Environment env;

    @PostConstruct
    public void init() {

        connectionUrl = env.getProperty("spring.datasource.url");
        user = env.getProperty("spring.datasource.username");
        psw = env.getProperty("spring.datasource.password");

        spark = SparkSession
                .builder()
                .master("local[16]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        connectDatabase();

    }

    public void connectDatabase() {


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

        employee.searchWithSpark(spark, connectionUrl, user, psw);

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


