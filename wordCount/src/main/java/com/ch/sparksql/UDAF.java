package com.ch.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author 渔郎
 * @CLassName UDAF
 * @Description TODO
 * @Date 2022/4/13 16:53
 */
public class UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("udaf");
        //配置join或者聚合操作整合shuffle数据时的分区数量
        conf.set("spark.sql.shuffle.partitions", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangSan", "liSi", "wangWu", "zhangSan", "liSi", "zhangSan"));
        JavaRDD<Row> map = parallelize.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> df = sqlContext.createDataFrame(map, structType);
        df.registerTempTable("user1");
        sqlContext.udf().register("StringCount",new UserDefinedAggregateFunction(){

            @Override
            public StructType inputSchema() {
                return DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("namexxx", DataTypes.StringType, true)));
            }

            @Override
            public StructType bufferSchema() {
                return DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("bfferxx", DataTypes.IntegerType, true)));
            }

            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,0);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0,buffer.getInt(0)+1);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer.getInt(0);
            }
        });

        sqlContext.sql("select name, StringCount(name) as strCount from user1 group by name").show();
    }
}
