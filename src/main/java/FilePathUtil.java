public class FilePathUtil {

    public static String getPath(String fileName) {
        if (null == fileName || fileName.length() == 0) {
            throw null;
        }

        return currentLoader().getResource(fileName).getPath();
    }

    public static ClassLoader currentLoader() {
        return Thread.currentThread().getContextClassLoader();
    }
}