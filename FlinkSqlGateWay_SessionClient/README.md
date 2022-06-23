1. 如果如下依赖下载不了
        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>flink-sql-gateway</artifactId>
            <version>0.3-SNAPSHOT</version>
        </dependency>

2. 需要自行编译打包flink-sql-gateway即可
   git clone https://github.com/ververica/flink-sql-gateway.git
   mvn clean install -DskipTests