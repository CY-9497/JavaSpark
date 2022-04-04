package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class GroupByKey {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("groupByKey");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangSan", "18"),
                new Tuple2<>("zhangSan", "188"),
                new Tuple2<>("liSi", "19"),
                new Tuple2<>("liSi", "d"),
                new Tuple2<>("wanWu", "c"),
                new Tuple2<>("wanWu", "b"),
                new Tuple2<>("maLiu", "a")
        ));
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangSan", 18),
                new Tuple2<>("zhangSan", 188),
                new Tuple2<>("liSi", 19),
                new Tuple2<>("liSi", 280),
                new Tuple2<>("wanWu", 38),
                new Tuple2<>("wanWu", 78),
                new Tuple2<>("maLiu", 78)
        ));

//        countByKey按key计数，countByValue按值计数
        Map<String, Long> stringLongMap = rdd.countByKey();
        Set<Map.Entry<String, Long>> entries = stringLongMap.entrySet();
        for(Map.Entry<String, Long> e:entries){
            String key = e.getKey();
            Long value = e.getValue();
            System.out.println(key +" " +value);
        }



//        reduce
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println(reduce);

//        zipWithIndex将RDD中元素与RDD的索引下标压缩成k，v格式的RDD
        JavaPairRDD<Tuple2<String, String>, Long> tuple2LongJavaPairRDD = rdd.zipWithIndex();

//        groupByKey将相同的key分组
        JavaPairRDD<String, Iterable<String>> groupByKey = rdd.groupByKey();
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2);
            }
        });
//        zip将两个分区数据量一致的RDD压缩成一个k，v格式的RDD
        JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer>> zip = rdd.zip(rdd1);
        zip.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, Integer>> tuple2Tuple2Tuple2) throws Exception {
                System.out.println(tuple2Tuple2Tuple2);
            }
        });

        sc.stop();
    }
}
