import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class HadoopHelper {

    public static void main(String[] args){
        String hdfsMaster = "hdfs://localhost:8020";
        FileSystem fs = HadoopHelper.getFileSystemInstance(hdfsMaster);
        renameFileBulk(fs,
                "/user/root/part*",
                "/user/part-",
                ".parquet");
    }

    public static FileSystem getFileSystemInstance(String masterUrl) {
        try {
            return FileSystem.get(new URI(masterUrl), new Configuration());
        } catch (Exception e) {
            e.getMessage();
            return null;
        }
    }

    public static boolean renameFile(FileSystem fs, String src, String dst) {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);

        try {
            return fs.rename(srcPath, dstPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void renameFileBulk(FileSystem fs, String srcFilePattern, String dstFilePrefix, String dstFileSuffix) {
        Path srcPath = new Path(srcFilePattern);

        try {
            Arrays.stream(fs.globStatus(srcPath)).forEach(fileStatus -> {
                Path dstPath = new Path(dstFilePrefix + System.currentTimeMillis() + dstFileSuffix);
                try {

                    fs.rename(fileStatus.getPath(), dstPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean delete(FileSystem fs, String file) {
        Path filePath = new Path(file);

        try {
            return fs.deleteOnExit(filePath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void close(FileSystem fs) {
        try {
            if (fs != null) fs.close();
        } catch (IOException exp) {
            exp.getMessage();
        }
    }
}
