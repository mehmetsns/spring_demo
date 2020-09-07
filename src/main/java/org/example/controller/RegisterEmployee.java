package org.example.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RegisterEmployee {

    int personel_id;
    String birth_date;
    String first_name;
    String last_name;
    String gender;
    String hire_date;
    String adding_Message;

    public void setPersonel_id(int personel_id) {
        this.personel_id = personel_id;
    }

    public void setBirth_date(String birth_date) {
        this.birth_date = birth_date;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setHire_date(String hire_date) {
        this.hire_date = hire_date;
    }

    public String _getAddingMessage() {
        return adding_Message;
    }

    public void register(Connection conn) {

        String Query = "insert into employees (emp_no,birth_date,first_name,last_name,gender,hire_date)\n" +
                "values (?, ?, ?, ?, ?, ?)";

        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(Query);
            ps.setInt(1, personel_id);
            ps.setDate(2, java.sql.Date.valueOf(birth_date));
            ps.setString(3, first_name);
            ps.setString(4, last_name);
            ps.setString(5, gender);
            ps.setDate(6, java.sql.Date.valueOf(hire_date));
            ps.executeUpdate();
            //ps.executeUpdate(Query);
            adding_Message = "Personel başarıyla eklendi";

        } catch (SQLException throwables) {
            adding_Message = throwables.getMessage();
        }

    }

}
