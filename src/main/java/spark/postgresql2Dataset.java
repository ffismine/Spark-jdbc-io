package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.utils.url_format;


public class postgresql2Dataset {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Mysql2Dataset")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // 建表操作：
//        CREATE TABLE salary_pg(
//                ID INT ,
//                NAME TEXT,
//                AGE INT,
//                SALARY INT
//        );


        Dataset<Row> dataset1 = spark.read().format("jdbc").options(url_format.postgresql_()).load();
        dataset1.show();
    }

}

