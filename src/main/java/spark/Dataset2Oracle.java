package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import spark.utils.schema;
import spark.utils.UrlFormat2;

import java.util.ArrayList;
import java.util.List;

public class Dataset2Oracle {

    public static void main(String[] args) {
        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create("11", "XXA", "25","5000"));
        list.add(RowFactory.create("12", "XXB", "24","6050"));

        SparkConf sparkConf = new SparkConf()
                .setAppName("Dataset2Mysql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> dataset = spark.createDataset(list, schema.ENC);
        dataset.write().mode(SaveMode.Append).format("jdbc").options(UrlFormat2.getUrl().oracle_()).save();
    }
}
