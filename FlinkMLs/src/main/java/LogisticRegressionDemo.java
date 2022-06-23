import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LogisticRegressionDemo {
    public static void main(String... args) {
        Configuration configuration = new Configuration();
        configuration.setString("iteration.data-cache.path", "file:///D:\\checkpoint");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Row> binomialTrainData =
                Arrays.asList(
                        Row.of(Vectors.dense(1, 2, 3, 4), 0., 1.),
                        Row.of(Vectors.dense(2, 2, 3, 4), 0., 2.),
                        Row.of(Vectors.dense(3, 2, 3, 4), 0., 3.),
                        Row.of(Vectors.dense(4, 2, 3, 4), 0., 4.),
                        Row.of(Vectors.dense(5, 2, 3, 4), 0., 5.),
                        Row.of(Vectors.dense(11, 2, 3, 4), 1., 1.),
                        Row.of(Vectors.dense(12, 2, 3, 4), 1., 2.),
                        Row.of(Vectors.dense(13, 2, 3, 4), 1., 3.),
                        Row.of(Vectors.dense(14, 2, 3, 4), 1., 4.),
                        Row.of(Vectors.dense(15, 2, 3, 4), 1., 5.));
        Collections.shuffle(binomialTrainData);

        Table binomialDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                binomialTrainData,
                                new RowTypeInfo(
                                        new TypeInformation[]{
                                                TypeInformation.of(DenseVector.class),
                                                Types.DOUBLE,
                                                Types.DOUBLE
                                        },
                                        new String[]{"features", "label", "weight"})));

        LogisticRegression logisticRegression = new LogisticRegression().setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(binomialDataTable);
        Table output = model.transform(binomialDataTable)[0];

        output.execute().print();
    }
}
