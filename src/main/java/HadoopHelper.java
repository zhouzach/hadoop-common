import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class HadoopHelper {

    private static Logger logger = LoggerFactory.getLogger(HadoopHelper.class);

    public static void main(String[] args) {
        String hdfsMaster = "hdfs://localhost:8020";
        FileSystem fs = HadoopHelper.getFileSystemInstance(hdfsMaster);
        renameFileBulk(fs,
                "/user/root/part*",
                "/user/part-",
                ".parquet");
    }

    public static void createFile(FileSystem fs, String file) {
        Path filePah = new Path(file);

        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(filePah);
            logger.info("create file: " + file);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (outputStream != null) {
                    // because of hdfs lease, the FSDataOutputStream out must close after creat the file
                    // otherwise can not append buffer into the file
                    outputStream.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    }

    public static void appendString2Hdfs(FileSystem fs, String content, String dst) {

        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.append(new Path(dst));
            outputStream.writeBytes(content);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (outputStream != null) {
                    // because of hdfs lease, the FSDataOutputStream out must close after creat the file
                    // otherwise can not append buffer into the file
                    outputStream.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void writeString2Hdfs(FileSystem fs, String content, String dst) {

        logger.info("Begin Write file into hdfs");
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(new Path(dst));
            outputStream.writeBytes(content);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (outputStream != null) {
                    // because of hdfs lease, the FSDataOutputStream out must close after creat the file
                    // otherwise can not append buffer into the file
                    outputStream.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        logger.info("End Write file into hdfs");
    }

    public static FileSystem getFileSystemInstance(String masterUrl) {
        try {
            return FileSystem.get(new URI(masterUrl), new Configuration());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    public static boolean renameFile(FileSystem fs, String src, String dst) {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);

        try {
            return fs.rename(srcPath, dstPath);
        } catch (IOException e) {
            logger.error(e.getMessage());
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
            logger.error(e.getMessage());
        }
    }

    public static boolean delete(FileSystem fs, String file) {
        Path filePath = new Path(file);

        try {
            return fs.delete(filePath, true);
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public static void close(FileSystem fs) {
        try {
            if (fs != null) fs.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
