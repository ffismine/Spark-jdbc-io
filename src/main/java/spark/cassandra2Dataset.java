package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public class cassandra2Dataset {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("readCassandra")
                .setMaster("local[*]");

        Map<String, String> map = new HashMap<>();

        // TODO: your keyspace
        map.put("keyspace","your keyspace");

        // TODO: your table
        map.put("table", "your table");
        // TODO: your connection.host
        map.put("spark.cassandra.connection.host","your connection.host");


        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> load = spark.read().options(map).format("org.apache.spark.sql.cassandra").load();
        load.show();
    }
}