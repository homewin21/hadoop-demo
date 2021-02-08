//package com.homewin.hadoopdemo.hdfs;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
//import org.springframework.context.annotation.Bean;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.Resource;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//
///**
// * @description: hdfs
// * @author: homewin
// * @create: 2021-02-08 09:53
// **/
//@Slf4j
//@Component
//public class HdfsUtil {
//    @Value("${hdfs.coreSite}")
//    private String coreSite;
//    @Value("${hdfs.hdfsSite}")
//    private String hdfsSite;
//    private FileSystem hdfs;
//
//    /**
//     * 获取hdfs
//     * @return hdfs
//     * @throws IOException ex
//     */
//    public FileSystem getHdfs() throws IOException {
//        if(hdfs==null){
//            Configuration conf = new Configuration();
//            conf.addResource(new FileInputStream(coreSite));
//            conf.addResource(new FileInputStream(hdfsSite));
//            hdfs=FileSystem.get(conf);
//            FileStatus[] fs = hdfs.listStatus(new Path("/"));
//            for (FileStatus f : fs) {
//                log.info("hdfs根目录下文件集:"+f.getPath().getName());
//            }
//        }
//        return hdfs;
//    }
//    /**
//     * @param localDir  本地文件夹
//     * @param remoteDir hdfs 远程文件
//     * @throws Exception ex
//     */
//    public void downloadDir(String localDir, String remoteDir, boolean delete) throws Exception {
//        FileSystem hfs = getHdfs();
//        File file = new File(localDir);
//        if (!file.exists()) {
//            if (!file.mkdirs()) {
//                throw new RuntimeException("cannot create dir : " + localDir);
//            }
//        }
//        Path remotePath = new Path(remoteDir);
//        if (!hfs.exists(remotePath) || !hfs.isDirectory(remotePath)) {
//            throw new RuntimeException(remoteDir + " is not a direct");
//        }
//        if (file.isFile()) {
//            throw new RuntimeException(localDir + " is a file!");
//        }
//        hfs.copyToLocalFile(delete, remotePath, new Path(localDir), true);
//    }
//}
