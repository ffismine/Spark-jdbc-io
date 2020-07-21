package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

import spark.utils.schema;
import spark.utils.url_format;

public class Dataset2Mysql {

    public static void main(String[] args) {
        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create("124", "xiaoming12", "22","5000"));
        list.add(RowFactory.create("125", "xiaohong12", "23","6050"));

        SparkConf sparkConf = new SparkConf()
                .setAppName("Dataset2Mysql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> dataset = spark.createDataset(list, schema.ENC);
        dataset.write().mode(SaveMode.Append).format("jdbc").options(url_format.mysql_()).save();


//        another method
//        Properties properties = new Properties();
//        properties.put("user", "xx");
//        properties.put("password", "xx");
//        properties.put("driver","com.mysql.jdbc.Driver");
//        dataset.write().mode(SaveMode.Append).jdbc("jdbc:mysql://xx.xx.xx.xx:3306/xx", "salary_mysql", properties);
    }
}
