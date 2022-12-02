import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        File jarFile = new File("D:\\idea\\flink-aaForMyFork\\flink\\flink-examples\\flink-examples-streaming\\target\\SocketWindowWordCount.jar");
        System.out.println(JarManifestParser.findEntryClass(jarFile).get());
    }
}
