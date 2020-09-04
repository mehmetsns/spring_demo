package org.example.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.ui.Model;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


@Controller
public class getEmployeeController {

    Connection conn = null;
    boolean isConnect = false;

    public void ConnectDatabase() {

        String connectionUrl = "jdbc:mysql://localhost:3306/employees";

        try {
            conn = DriverManager.getConnection(connectionUrl, "root", "123");
            isConnect = true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            isConnect = false;
        }

    }

    @RequestMapping(method = RequestMethod.GET, value = "/employee/search")

    @ResponseBody()
    public Employee searchEmployee(@RequestBody Employee employee) {

        if (!isConnect)
            ConnectDatabase();

        employee.searchEmployee(conn);

        return employee;
    }


}


