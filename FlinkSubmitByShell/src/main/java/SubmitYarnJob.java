import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 注意：需要提前保证Flink已经适配Hadoop环境。
 *
 * @author FelixZh
 */
public class SubmitYarnJob {

    public static void main(String[] args) throws Exception {

        String FLINK_HOME = "/home/myHadoopCluster/flink-1.13.6";
        //String RUN_MODE = "YARN_PER_JOB";
        String RUN_MODE = "YARN_APPLICATION";
        String JAR_PATH = "/home/myHadoopCluster/flink-1.13.6/examples/batch/WordCount.jar";

        //步骤1：准备命令
        String submitCmd = prepareSubmitCmd(FLINK_HOME, RUN_MODE, JAR_PATH);
        System.out.println(String.format("submitCmd: %s", submitCmd));

        //步骤2：执行命令
        String applicationId = localExecSubmitCmd(submitCmd);
        System.out.println(String.format("applicationId: %s", applicationId));
    }

    /**
     * 根据具体环境、组装flink脚本提交命令
     *
     * @param flinkHome Flink路径
     * @param runMode   运行模式
     * @param jarPath   业务jar路径
     */
    public static String prepareSubmitCmd(String flinkHome, String runMode, String jarPath) {
        StringBuilder submitCmd = new StringBuilder();
        submitCmd.append(flinkHome);

        switch (runMode) {
            case "YARN_PER_JOB":
                submitCmd.append("/bin/flink run -t yarn-per-job ");
                break;
            case "YARN_APPLICATION":
                submitCmd.append("/bin/flink run-application -t yarn-application ");
                break;
            default:
                throw new RuntimeException(String.format("Dont't support run mode: %s, only support YARN_PER_JOB/YARN_APPLICATION", runMode));
        }

        submitCmd.append(jarPath);
        return submitCmd.toString();
    }

    /**
     * 在本地节点执行提交命令，需要本地节点部署Flink环境
     *
     * @param submitCmd 命令
     */
    public static String localExecSubmitCmd(String submitCmd) throws IOException {
        String line;
        String result = null;
        String applicationId;
        String applicationIdLineFlag = "Submitted application";

        Process process = Runtime.getRuntime().exec(submitCmd);
        InputStream inputStream = process.getInputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            if (line.contains(applicationIdLineFlag)) {
                System.out.println(String.format("line: %s", line));
                result = line.split(applicationIdLineFlag)[1];
                break;
            }
        }

        if (result == null) {
            throw new RuntimeException("Can't find applicationId. Maybe submit failed.");
        }

        applicationId = result.trim();
        return applicationId;
    }

}
