package org.example.controller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SearchEmployee {

    int personelId;
    Date startDate;
    Date endDate;
    List<String> jobs;
    String message;

    private Logger logger = Logger.getLogger(SearchEmployee.class.getName());

    public void setPersonelId(int personelId) {
        this.personelId = personelId;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public int getPersonelId() {

        return personelId;
    }

    public Date getStartDate() {

        return startDate;
    }

    public Date getEndDate() {

        return endDate;
    }

    public List<String> getJobs() {

        return jobs;
    }

    public String getMessage() {
        return message;
    }

    public void search(Connection conn) {

        jobs = new ArrayList<>();

        String query = "SELECT e.emp_no,t.title,t.from_date,t.to_date\n" +
                "FROM employees e join titles t on e.emp_no=t.emp_no\n" +
                "where e.emp_no=?" +
                " and t.from_date >=?" +
                " and t.to_date<=?";

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(query);

            ps.setInt(1, personelId);
            ps.setDate(2, startDate);
            ps.setDate(3, endDate);

            rs = ps.executeQuery();

            logger.info("Sql Execution is finished");

            while (rs.next()) {
                jobs.add(rs.getString(2));
            }

            if (!jobs.isEmpty()) {
                message = "Personel Bulundu";
            } else
                message = "Arama kriterlerine uyan bir personel yok";


        } catch (SQLException e) {
            message = "Sorguda hata var";
            // handle the exception
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                message = e.getMessage();
            }

            try {
                if (rs != null) rs.close();
            } catch (SQLException e) {
                message = e.getMessage();
            }
        }


    }

    public void searchWithSpark(SparkSession spark) {


        String sql = String.format("(SELECT e.emp_no,t.title,t.from_date,t.to_date\n" +
                "FROM employees e join titles t on e.emp_no=t.emp_no\n" +
                "where e.emp_no=%d" +
                " and t.from_date >= '%s'" +
                " and t.to_date<= '%s') as employees", personelId, startDate, endDate);


        Row[] dataRows = null;
        try {

            Dataset<Row> dataFrame = spark.sqlContext()
                    .read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/employees")
                    .option("user", "root")
                    .option("password", "123")
                    .option("dbtable", sql)
                    .load();

            dataRows = (Row[]) dataFrame.collect();

            jobs = new ArrayList<>();
            for (Row row : dataRows) {

                jobs.add(row.get(1).toString());
            }

            if (!jobs.isEmpty()) {
                message = "Personel Bulundu";
            } else
                message = "Arama kriterlerine uyan bir personel yok";

        } catch (Exception e) {
            message = e.getMessage();
        }

    }
}