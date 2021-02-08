package com.homewin.hadoopdemo.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @description: hdfs
 * @author: homewin
 * @create: 2021-02-08 09:53
 **/
@Slf4j
public class HdfsUtil {
    private FileSystem hdfs;
    /**
     * 一级文件信息列表
     */
    private List<FileStatus> fsList = new LinkedList<>();

    public synchronized FileSystem getHdfs() {
        if (hdfs == null) {
            try {
                log.info("初始化 hdfs");
                System.setProperty("HADOOP_USER_NAME", "edc_base");
                //会自动加载工程路径下的配置文件
                Configuration conf = new Configuration();
//                conf.addResource("hdfs-site.xml");
//                conf.addResource("core-site.xml");
                hdfs = FileSystem.get(conf);
                fsList = Arrays.asList(hdfs.listStatus(new Path("/")));

            } catch (Throwable e) {
                log.info("创建hdfs 失败", e);
                throw new RuntimeException("创建hdfs 失败:", e);
            }
        }
        return hdfs;
    }

    /**
     * @param localDir  本地文件夹
     * @param remoteDir hdfs 远程文件
     * @throws Exception
     */
    public void downloadDir(String localDir, String remoteDir, boolean delete) throws Exception {
        FileSystem hfs = getHdfs();
        File file = new File(localDir);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new RuntimeException("cannot create dir : " + localDir);
            }
        }
        Path remotePath = new Path(remoteDir);
        if (!hfs.exists(remotePath) || !hfs.isDirectory(remotePath)) {
            throw new RuntimeException(remoteDir + " is not a direct");
        }
        if (file.isFile()) {
            throw new RuntimeException(localDir + " is a file!");
        }
        hfs.copyToLocalFile(remotePath, new Path(localDir));
    }

    private static void mergeFile(String tempDir, String finalFile) throws Exception {
        //此处只是合并
        log.info("合并文件：{} to {}", tempDir, finalFile);
        try (FileOutputStream fileOutputStream = new FileOutputStream(finalFile);
             FileChannel fcOunt = fileOutputStream.getChannel();) {
            File dirs = new File(tempDir);
            File[] infilelist = dirs.listFiles();
            for (File file : infilelist) {
                FileInputStream fis = new FileInputStream(file);
                fis.getChannel().transferTo(0, fis.available(), fcOunt);
                fis.close();
            }
        } catch (Exception e) {
            log.error("合并文件失败", e);
        }
    }

    private static void rmr(File f) {
        if (f.isFile()) {
            f.delete();
        } else {
            for (File subf : f.listFiles()) {
                rmr(subf);
            }
            f.delete();
        }
    }

    public void downloadAndMerge(String singleFile, String remote) throws Exception {
        downloadAndMerge(singleFile, remote, false);
    }

    public void downloadAndMerge(String singleFile, String remote, boolean delete) throws Exception {
        String tmp = singleFile + "_tmpdir";
        try {
            downloadDir(tmp, remote, delete);
            File[] dirs = new File(tmp).listFiles();
            if (dirs.length > 0) {
                mergeFile(dirs[0].getCanonicalPath(), singleFile);
            } else {
                throw new RuntimeException("下载失败");
            }
        } finally {
            rmr(new File(tmp));
        }
    }

    /**
     * @param local
     * @param remote
     * @throws Exception
     */
    public void uploadFile(String local, String remote) throws Exception {
        getHdfs().copyFromLocalFile(false, true, new Path(local), new Path(remote));
    }

    public List<FileStatus> getFsList() {
        if (fsList.isEmpty()) {
            getHdfs();
        }
        return fsList;
    }

}
