package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaSparkWordCount {
    public static void main(String[] args){
        /**
         * conf
         * 1.可以设置spark的运行模式
         * 2.可以设置spark在webui中的application的名称
         * 3.可以设置spark在application运行的资源
         *
         * spark的运行模式
         * 1.local--本地模式，多用于测试
         * 2.stanalone ---spark 自带的资源调度框架，支持分布式搭建，spark任务可以依赖stanalone调度资源
         * 3.yarn --hadoop生态圈中资源调度框架，spark也可以依赖yarn调度资源
         * 4.mesos--资源调度框架
         */
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
//        conf.set("spark.submit.deployMode","client");
//        sparkcontext通往集群的唯一通道
        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.textFile读取文件
        JavaRDD<String> lines = sc.textFile("./words");
//        lines.flatMap 进一条数据出多条数据
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override

            public Iterator<String> call(String s) throws Exception {
                return (Iterator<String>) ((Arrays.asList(s.split(" "))).iterator());
            }
        });
//        将某rdd格式转化为k,v格式使用xxxToPair()   进一个出一个元组
        JavaPairRDD<String, Integer> PairWord = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String,Integer> result =  PairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
//        按值排序，先key和value交换值
         JavaPairRDD<Integer,String> result1 = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
             @Override
             public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                 String s = stringIntegerTuple2._1;
                 Integer I = stringIntegerTuple2._2;
                 return new Tuple2<>(I,s);
             }
         });
//         按key排序
        result1 = result1.sortByKey(false);
//        赋值给result
        result = result1.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2,tuple2._1);
            }
        });
//        显示
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
        sc.stop();
    }
}
