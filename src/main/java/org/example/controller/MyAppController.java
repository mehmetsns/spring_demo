package org.example.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.ui.Model;
import org.apache.commons.lang3.StringUtils;


@Controller
public class MyAppController {
    @RequestMapping({"/"})
    public String infoForm(Model model) {
        model.addAttribute("info", new Info());
        return "home";
    }

    @PostMapping("/")
    public String infoSubmit(@ModelAttribute Info info, Model model) {
        model.addAttribute("info", info);

        info.setContent(Boolean.toString(StringUtils.isEmpty(info.getContent())));
        return "result";
    }



}


