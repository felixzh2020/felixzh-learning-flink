import java.util.Locale;

public class OperatingSystem {
    public OperatingSystem() {
    }

    public static final String NAME;
    public static final boolean IS_WINDOWS;

    static {
        NAME = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        IS_WINDOWS = NAME.startsWith("windows");
    }

    public static void main(String [] args){
        System.out.println(OperatingSystem.NAME);
        System.out.println(OperatingSystem.IS_WINDOWS);
    }
}
