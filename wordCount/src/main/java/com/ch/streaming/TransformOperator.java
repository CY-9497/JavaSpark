package com.ch.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author 渔郎
 * @CLassName TransformOperator
 * @Description TODO
 * @Date 2022/4/20 14:50
 */

public class TransformOperator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("transformTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(5));

        //黑名单
        List<String> list = Arrays.asList("zhangSan");
        final Broadcast<List<String>> broadcast = streamingContext.sparkContext().broadcast(list);

        //接受socket数据源
        JavaReceiverInputDStream<String> hadoop001 = streamingContext.socketTextStream("hadoop001", 9999);
        JavaPairDStream<String,String> map = hadoop001.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], s);
            }
        });
        JavaDStream<String> transform = map.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> javaPairRDD) throws Exception {
                JavaPairRDD<String, String> filter = javaPairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                        return !broadcast.value().contains(tuple2._1);
                    }
                });
                JavaRDD<String> map1 = filter.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        return tuple2._2;
                    }
                });
                return map1;
            }
        });
        transform.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
