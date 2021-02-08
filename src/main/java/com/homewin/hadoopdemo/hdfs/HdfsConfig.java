package com.homewin.hadoopdemo.hdfs;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @description:
 * @author: homewin
 * @create: 2021-02-08 15:19
 **/
@ConfigurationProperties(prefix = "hadoop")
@Configuration
public class HdfsConfig {
    @Bean
    public HdfsUtil getHdfsUtil() {
        HdfsUtil hdfsUtil = new HdfsUtil();
        hdfsUtil.getHdfs();
        return hdfsUtil;
    }
}
