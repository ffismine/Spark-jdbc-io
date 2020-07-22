package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.utils.UrlFormat2;



public class cassandra2Dataset {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("readCassandra")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> load = spark.read().options(UrlFormat2.getUrl().cassandra_()).format("org.apache.spark.sql.cassandra").load();
        load.show();
    }
}