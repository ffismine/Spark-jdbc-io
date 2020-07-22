package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.utils.UrlFormat2;

import java.util.Properties;


public class Mysql2Dataset {

/** https://www.cnblogs.com/wcgstudy/p/10984550.html

 */

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Mysql2Dataset")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();


        // jdbc:mysql://10.19.180.60:3306/hktest
        // 建表操作：

        //1
        // CREATE TABLE salary_mysql
        //( id INT(11),
        //name VARCHAR(25),
        //age INT(3),
        //salary FLOAT);


        //2
        //CREATE TABLE salary_level_mysql
        //( id INT(11),
        //salary_level INT(3));


        // 第一种方式
        Properties properties = new Properties();
        // TODO: your user and pw
        properties.put("user", "xx");
        properties.put("password", "xx");

        // TODO: your Connection string
        Dataset<Row> dataset = spark.read().jdbc("jdbc:mysql://xx.xx.xx.xx:3306/xx","salary_mysql",properties);
        dataset.show();

        //第一种方式查询
        spark.read().jdbc("jdbc:mysql://xx.xx.xx.xx:3306/xx",
                "(select salary_mysql.name,salary_mysql.age,salary_level_mysql.salary_level " +
                        "from salary_mysql,salary_level_mysql where salary_mysql.id = salary_level_mysql.id) A",properties).show();



        //第二种方式
        Dataset<Row> dataset1 = spark.read().format("jdbc").options(UrlFormat2.getUrl().mysql_()).load();
        dataset1.show();
    }


}
