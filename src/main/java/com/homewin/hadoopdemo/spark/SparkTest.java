package com.homewin.hadoopdemo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @author: homewin
 * @create: 2021-02-09 16:20
 **/
public class SparkTest {
    private static String appName = "spark.demo";
    private static String master = "local[*]";

    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            //初始化 JavaSparkContext
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            sc = new JavaSparkContext(conf);

            // 构造数据源
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

            //并行化创建rdd
            JavaRDD<Integer> rdd = sc.parallelize(data);

            //map && reduce
            Integer result = rdd.map((Function<Integer, Integer>) integer -> integer).reduce((Function2<Integer, Integer, Integer>) Integer::sum);

            System.out.println("执行结果：" + result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
