package com.ch.sparksql;

import com.ch.domain.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author 渔郎
 * @CLassName CreateDfFromRDD
 * @Description TODO
 * @Date 2022/4/12 9:19
 */
public class CreateDfFromRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("createDfFromRdd");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> stringJavaRDD = sc.textFile("./person.txt");
        JavaRDD<Person> map = stringJavaRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                Person person = new Person();
                person.setId(Integer.parseInt(s.split(",")[0]));
                person.setName(s.split(",")[1]);
                person.setAge(Integer.parseInt(s.split(",")[2]));
                return person;
            }
        });
        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, Person.class);
        dataFrame.show();
        dataFrame.printSchema();
        dataFrame.registerTempTable("person");
        Dataset<Row> sql = sqlContext.sql("select * from person where age > 18");
        sql.show();
        sc.stop();
    }
}
