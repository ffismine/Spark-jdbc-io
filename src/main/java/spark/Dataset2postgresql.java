package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

import spark.utils.schema;
import spark.utils.UrlFormat2;

public class Dataset2postgresql {

    public static void main(String[] args) {
        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create(4, "ad", 22, 5000));
        list.add(RowFactory.create(12, "ae", 23, 6050));

        SparkConf sparkConf = new SparkConf()
                .setAppName("Dataset2Mysql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> dataset = spark.createDataset(list, schema.ENC);
        dataset.write().mode(SaveMode.Append).format("jdbc").options(UrlFormat2.getUrl().postgresql_()).save();

    }

}
