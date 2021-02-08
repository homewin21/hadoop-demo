package com.homewin.hadoopdemo.controller;

import com.homewin.hadoopdemo.service.TestService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * @description:
 * @author: homewin
 * @create: 2021-02-08 11:01
 **/
@RestController
@Slf4j
@RequestMapping("/test")
public class TestController {
    @Autowired
    private TestService testService;

    @RequestMapping("/getFsList")
    public List<FileStatus> getFsList() throws IOException {
        return testService.getFsList();
    }
}
