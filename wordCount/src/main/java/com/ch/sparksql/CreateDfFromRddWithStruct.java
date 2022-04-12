package com.ch.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * @author 渔郎
 * @CLassName createDfFromRddWithStruct
 * @Description TODO
 * @Date 2022/4/12 14:58
 */
public class CreateDfFromRddWithStruct {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("createDfFromRdd");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> stringJavaRDD = sc.textFile("./person.txt");
        JavaRDD<Row> map = stringJavaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        Integer.parseInt(s.split(",")[0]),
                        s.split(",")[1],
                        Integer.parseInt(s.split(",")[2])
                );
            }
        });
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType field = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, field);
        dataFrame.show();
        sc.stop();
    }
}
