package com.ch.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

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
        //第一种方式读取mysql表数据
        HashMap<String, String> map = new HashMap<>();
        map.put("url","jdbc:mysql://192.168.100.128:3306/spark");
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("user","root");
        map.put("password","123456");
        map.put("dbtable","user");

        Dataset<Row> person = sqlContext.read().format("jdbc").options(map).load();

        person.show();
        person.registerTempTable("user1");

        sqlContext.udf().register("strLen", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        },DataTypes.IntegerType);
        //map.put("dbtable","score")
        Dataset<Row> sql = sqlContext.sql("select name,strLen(name) as length from user1");
        sql.show();

        //第二种方式
//        DataFrameReader jdbc = sqlContext.read().format("jdbc");
//        jdbc.option("url","jdbc:mysql://192.168.100.128:3306/spark");
//        jdbc.option("driver","com.mysql.jdbc.Driver");
//        jdbc.option("user","root");
//        jdbc.option("password","123456");
//        jdbc.option("dbtable","score");
//        Dataset<Row> load = jdbc.load();
//        load.show();

//        load.registerTempTable("score1");
//        将Dataframe两个表合并的结果存入mysql
//        Properties properties = new Properties();
//        properties.setProperty("user","root");
//        properties.setProperty("password","123456");
//        result.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.100.128:3306/spark","result",properties);

        sc.stop();
    }
}
