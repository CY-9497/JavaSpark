package com.ch.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author 渔郎
 * @CLassName SqlTest
 * @Description TODO
 * @Date 2022/4/12 8:47
 */
public class SqlTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("sqlTest");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.read().format("json").load("./json");
        df.show();
        df.printSchema();

        JavaRDD<Row> df1 = df.javaRDD();
        df1.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
//                System.out.println(row);
//                System.out.println(row.get(1));
                System.out.println((String) row.getAs("name"));
            }
        });

        //将Dataset注册成临时表
        //t1表不在磁盘也不在内存中，相当于一个指针指向源文件底层操作解析soarj job读取源文件
        df.registerTempTable("t1");
        Dataset<Row> sql = sqlContext.sql("select * from t1 where age > 18");
        sql.show();

        sc.stop();
    }
}
