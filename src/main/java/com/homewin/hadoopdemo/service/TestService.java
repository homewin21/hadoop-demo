package com.homewin.hadoopdemo.service;

import com.homewin.hadoopdemo.hdfs.HdfsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @description:
 * @author: homewin
 * @create: 2021-02-08 10:59
 **/
@Service
@Slf4j
public class TestService {
    @Autowired
    private HdfsUtil hdfsUtil;

    public List<FileStatus> getFsList() throws IOException {
        List<FileStatus> fsList = hdfsUtil.getFsList();
        for (FileStatus fs : fsList) {
            fs.setSymlink(new Path("1"));
        }
        return fsList;
    }
}
