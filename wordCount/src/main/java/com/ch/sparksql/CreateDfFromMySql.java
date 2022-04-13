package com.ch.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * @author 渔郎
 * @CLassName CreateDfFromMySql
 * @Description TODO
 * @Date 2022/4/12 15:28
 */
public class CreateDfFromMySql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mysql");
        //配置join或者聚合操作整合shuffle数据时的分区数量
        conf.set("spark.sql.shuffle.partitions", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        HashMap<String, String> map = new HashMap<>();
        map.put("url","jdbc:mysql://192.168.100.128:3306/spark");
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("user","root");
        map.put("password","Cy715809.");
        map.put("dbtable","user");

        Dataset<Row> person = sqlContext.read().format("jdbc").options(map).load();
        person.show();
        person.registerTempTable("user1");
        Dataset<Row> sql = sqlContext.sql("select * from user1");
        sql.show();
        sc.stop();
    }
}
