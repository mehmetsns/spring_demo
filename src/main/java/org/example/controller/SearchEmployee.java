package org.example.controller;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SearchEmployee {

    int personel_id;
    String start_date;
    String end_date;
    List<String> jobs;
    String Message;


    public void setPersonel_id(int personel_id) {
        this.personel_id = personel_id;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public int getPersonel_id() {

        return personel_id;
    }

    public String getStart_date() {

        return start_date;
    }

    public String getEnd_date() {

        return end_date;
    }

    public List<String> getJobs() {

        return jobs;
    }

    public String getMessage() {
        return Message;
    }

    public void search(Connection conn) {

        jobs = new ArrayList<String>();

        String from_date = "'" + start_date + "'";
        String to_date = "'" + end_date + "'";
        String emp_id = Integer.toString(personel_id);

        String Query = "SELECT e.emp_no,t.title,t.from_date,t.to_date\n" +
                "FROM employees e join titles t on e.emp_no=t.emp_no\n" +
                "where e.emp_no=" + emp_id +
                " and t.from_date >=" + from_date +
                " and t.to_date<=" + to_date;

        try (
                PreparedStatement ps = conn.prepareStatement(Query);

                ResultSet rs = ps.executeQuery()) {

            System.out.print("Execution is finished \n");

            ResultSetMetaData rsmd = rs.getMetaData();
            int column_count = rsmd.getColumnCount();

            for (int i = 1; i <= column_count; i++)
                System.out.format("%-25s", rsmd.getColumnName(i));
            // System.out.print("\t" + rsmd.getColumnName(i));

            while (rs.next()) {

                jobs.add(rs.getString(2));

                System.out.println("");
                for (int i = 1; i <= column_count; i++) {
                    String columnValue = rs.getString(i);
                    System.out.format("%-25s", columnValue);
                }


            }

            if (jobs.size() > 0) {
                Message = "Personel Bulundu";
            } else
                Message = "Arama kriterlerine uyan bir personel yok";


        } catch (SQLException e) {
            Message = "Sorguda hata var";
            // handle the exception
        }


    }

}