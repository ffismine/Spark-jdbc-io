package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import spark.utils.schema;
import spark.utils.url_format;


public class Dataset2Cassandra {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeCassandra")
                .setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create("124", "xiaoming12", "22","5000"));
        list.add(RowFactory.create("125", "xiaohong12", "23","6050"));

        Dataset<Row> dataset = spark.createDataset(list, schema.ENC);
        for (String field : dataset.schema().fieldNames()) {
            dataset = dataset.withColumnRenamed(field,field.toLowerCase());
        }
        dataset.write().mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(url_format.cassandra_()).save();
    }

}