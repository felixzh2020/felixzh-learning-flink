# 项目背景
本项目属于Apache Flink实战案例，持续维护更新。

# 前置条件
Java、Maven、Git。

# 编译、打包、运行
分布式环境搭建相对繁琐，在IDE(idea)调试运行minicluster。如需提交到standalone、yarn等环境可以直接使用命令运行编译、打包的jar。mvn clean package -DskipTests -Dfast
注意：本地IDE运行和远程提交的pom.xml中依赖scope不同，注意自行调整

# 案例列表
1. flink canal-json格式使用 https://github.com/felixzh2020/felixzh-learning-flink/tree/master/format/src/main/java/com/felixzh/learning/flink/format/canal_json
2. flink debezium-json格式使用 https://github.com/felixzh2020/felixzh-learning-flink/tree/master/format/src/main/java/com/felixzh/learning/flink/format/debezium_json

# 微信公众号
欢迎关注微信公众号(大数据从业者)，期待共同交流沟通、共同成长！

# 博客地址
https://www.cnblogs.com/felixzh/
