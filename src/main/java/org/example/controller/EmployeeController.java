package org.example.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


@Controller
public class EmployeeController {

    Connection conn = null;
    boolean isConnect = false;

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


    @PostMapping(value = "/employee/search")

    @ResponseBody()
    public String registerEmployee(@RequestBody RegisterEmployee employee) {

        if (!isConnect)
            connectDatabase();

        employee.register(conn);

        return employee.alertRegisterMesage();
    }


}


