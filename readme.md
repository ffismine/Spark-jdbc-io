### 两种单例模式，以oracle连接为例
 
#### urlformat1: 枚举类型
Dataset<Row> dataset = spark.read().format("jdbc").options(UrlFormat1.urlFormat.oracle_()).load();

#### urlformat2: 饿汉式类型
Dataset<Row> dataset = spark.read().format("jdbc").options(UrlFormat2.getUrl().oracle_()).load();

项目里用的是第二种类型