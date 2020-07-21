package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.utils.url_format;


public class Oracle2Dataset {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Mysql2Dataset")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // 建表操作：

//        create table salary_oracle
//                (
//                        id number(11),
//                        name VARCHAR(25),
//                        age number(3),
//                        salary number(10)
//                );

        Dataset<Row> dataset = spark.read().format("jdbc").options(url_format.oracle_()).load();
        dataset.show();
    }
}