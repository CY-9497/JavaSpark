package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FilterTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkFilterTest");
        //        conf.set("spark.submit.deployMode","client");
//        sparkcontext通往集群的唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);
        //        sc.textFile读取文件
        JavaRDD<String> lines = sc.textFile("./words");

        JavaRDD<String> filter = lines.filter(new Function<String, Boolean>() {

            @Override
            public Boolean call(String s) throws Exception {
                return s.equals("hello ChenYun");
            }
        });
        filter.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        long count = filter.count();
        System.out.println(count);

    }
}
