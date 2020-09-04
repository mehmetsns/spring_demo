package org.example.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.ui.Model;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


@Controller
public class getEmployeeController {

    @RequestMapping(method = RequestMethod.GET, value="/employee/search")

    @ResponseBody()
    public Employee searchEmployee(@RequestBody Employee employee) {

        employee.searchEmployee();


        return employee;
    }




}


