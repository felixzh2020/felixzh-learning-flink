import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class NCFlinkSqlUDFIdea {
    public static void main(String... args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //注册的Function名称自定义任意值
        tEnv.createTemporarySystemFunction("JsonFunction1", JsonFunction.class);

        // execute a Flink SQL job and print the result locally
       /* tEnv.executeSql(
                // define the aggregation
                "SELECT JsonFunction(word, 'words'), frequency\n"
                        // read from an artificial fixed-size table with rows and columns
                        + "FROM (\n"
                        + "  VALUES ('Hello', 1), ('Ciao', 1), ('felixzh', 2)\n"
                        + ")\n"
                        // name the table and its columns
                        + "AS WordTable(word, frequency)\n")
                .print();*/

        // execute a Flink SQL job and print the result locally
        tEnv.executeSql(
                // define the aggregation
                "SELECT JsonFunction1(word), frequency\n"
                        // read from an artificial fixed-size table with rows and columns
                        + "FROM (\n"
                        + "  VALUES ('Hello', 1), ('Ciao', 1), ('felixzh', 2)\n"
                        + ")\n"
                        // name the table and its columns
                        + "AS WordTable(word, frequency)\n")
                .print();
    }

    /**
     * 标量函数：必要有eval方法。
     * 方法名必须为eval，输入参数类型和个数自定义、返回值类型自定义
     */
    public static class JsonFunction extends ScalarFunction {
        /*public String eval(String line, String key) {
            return line + ":" + key;

        }*/

        public String eval(String line) {
            return line + ":" + line;

        }

       /* public long eval(String line) {
            return (line + ":" + line).hashCode();

        }*/
    }
}
