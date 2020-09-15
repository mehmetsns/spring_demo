package org.example.model;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.from_json;

public class RegisterEmployee implements Serializable {

    int personelId;
    String birthDate;
    String firstName;
    String lastName;
    String gender;
    String hireDate;
    String message;


    public void setPersonelId(int personelId) {
        this.personelId = personelId;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public int getPersonelId() {

        return personelId;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getGender() {
        return gender;
    }

    public String getHireDate() {
        return hireDate;
    }

    public String alertMessage() {
        return message;
    }

    public void register(Connection conn) {


        String query = "insert into employees (emp_no,birth_date,first_name,last_name,gender,hire_date)\n" +
                "values (?, ?, ?, ?, ?, ?)";

        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(query);
            ps.setInt(1, personelId);
            ps.setDate(2, java.sql.Date.valueOf(birthDate));
            ps.setString(3, firstName);
            ps.setString(4, lastName);
            ps.setString(5, gender);
            ps.setDate(6, java.sql.Date.valueOf(hireDate));
            ps.executeUpdate();

            message = "Personel başarıyla eklendi";

        } catch (SQLException throwables) {
            message = throwables.getMessage();
        } finally {

            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    message = throwables.getMessage();
                }
            }
        }
    }

    public void registerWithSpark(SparkSession spark) {


        JavaSparkContext javaSc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<RegisterEmployee> list = new ArrayList<>();
        list.add(this);

        JavaRDD<RegisterEmployee> personsRDD = javaSc.parallelize(list, 1);
        Dataset<Row> personelDf = spark.createDataFrame(personsRDD, RegisterEmployee.class);
        personelDf = personelDf.withColumnRenamed("personelId", "emp_no")
                .withColumnRenamed("birthdate", "birth_date")
                .withColumnRenamed("firstname", "first_name")
                .withColumnRenamed("lastname", "last_name")
                .withColumnRenamed("hireDate", "hire_date");

        try {
            personelDf.write()
                    .mode(SaveMode.Append)
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/employees")
                    .option("dbtable", "employees")
                    .option("user", "root")
                    .option("password", "123")
                    .save();

            message = "Personel başarıyla eklendi";
        } catch (Exception e) {
            message = e.getMessage();
        }

    }

    public void sendKafka(SparkSession spark) {


        JavaSparkContext javaSc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<RegisterEmployee> list = new ArrayList<>();
        list.add(this);

        JavaRDD<RegisterEmployee> personsRDD = javaSc.parallelize(list, 1);
        Dataset<Row> personelDf = spark.createDataFrame(personsRDD, RegisterEmployee.class);
        personelDf = personelDf.withColumnRenamed("personelId", "emp_no")
                .withColumnRenamed("birthdate", "birth_date")
                .withColumnRenamed("firstname", "first_name")
                .withColumnRenamed("lastname", "last_name")
                .withColumnRenamed("hireDate", "hire_date");

        //personelDf.show();

        try {


            personelDf.selectExpr("to_json(struct(*)) AS value")
                    .write()
                    .mode(SaveMode.Append)
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "postEmployee")
                    .option("checkpointLocation", "/usr/local/spark/chkpoint/")
                    .save();


            message = "Personel başarıyla eklendi";
        } catch (Exception e) {
            message = e.getMessage();
        }


    }

}
