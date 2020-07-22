package spark.utils;

import java.util.HashMap;
import java.util.Map;

// 两种单例模式之二：饿汉式


public class UrlFormat2 {


    private static final UrlFormat2 urlFormat = new UrlFormat2();

    private UrlFormat2() {}

    public static UrlFormat2 getUrl(){  // 获取实例
        return urlFormat;
    }

    public Map<String, String> mysql_(){

        Map<String, String> optionMap = new HashMap<String, String>();
        // TODO: your Connection string with pw
        optionMap.put("url","jdbc:mysql://xx.xx.xx.xx:3306/xx?userSSl=false&user=xx&password=xx");
        optionMap.put("driver", "com.mysql.jdbc.Driver");
        optionMap.put("dbtable", "salary_mysql");

        return optionMap;
    }

    public Map<String, String> cassandra_(){

        Map<String, String> optionMap = new HashMap<String, String>();
        // TODO: your keyspace
        optionMap.put("keyspace","your keyspace");
        // TODO: your table
        optionMap.put("table", "your table");
        // TODO: your connection.host
        optionMap.put("spark.cassandra.connection.host","your connection.host");
        optionMap.put("spark.cassandra.connection.port", "9042"); //will be set to all hosts if no individual ports are given

        return optionMap;
    }

    public Map<String, String> postgresql_(){
        Map<String, String> optionMap = new HashMap<String, String>();
        // TODO your Connection string
        optionMap.put("url","jdbc:postgresql://xx.xx.xx.xx:5432/xx?userSSl=false&user=xx&password=xx");
        optionMap.put("driver", "org.postgresql.Driver");
        //  optionMap.put("user", "xx");
        //   optionMap.put("password", "xx");
        optionMap.put("dbtable", "salary_pg");

        return optionMap;
    }

    public Map<String, String> oracle_(){
        Map<String, String> optionMap = new HashMap<String, String>();
        // 注意 host:port:sid
        // TODO: your Connection string
        optionMap.put("url", "jdbc:oracle:thin://xx:x@xx.xx.xx.xx:1521/xx");
        optionMap.put("driver", "oracle.jdbc.driver.OracleDriver");
        // TODO: your user, your pw, your dbtable
        optionMap.put("user", "your user");
        optionMap.put("password", "your pw");
        optionMap.put("dbtable", "your dbtable");

        return optionMap;
    }

}


