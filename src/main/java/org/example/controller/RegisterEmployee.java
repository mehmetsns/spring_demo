package org.example.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RegisterEmployee {

    int personelId;
    String birthDate;
    String firstName;
    String lastName;
    String gender;
    String hireDate;
    String addingMessage;

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

    public String alertRegisterMesage() {
        return addingMessage;
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

            addingMessage = "Personel başarıyla eklendi";

        } catch (SQLException throwables) {
            addingMessage = throwables.getMessage();
        }

        finally{

            if (ps !=null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    addingMessage = throwables.getMessage();
                }
            }
        }
    }

}
