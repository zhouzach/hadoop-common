import org.apache.spark.rdd.PartitionCoalescer;
import org.apache.spark.rdd.PartitionGroup;
import org.apache.spark.sql.SparkSession;
import scala.Option;

public class SparkUtil {

    public static void mergeFile() {
        SparkSession spark = SparkSession.builder()
//                .master("org.rabbit.spark://localhost:7077")
                .master("local")
                .getOrCreate();


        spark.sparkContext().textFile("hdfs://localhost:8020/user/root/data/", 1)
                .coalesce(1, false,
                        Option.apply((PartitionCoalescer) (maxPartitions, parent) -> new PartitionGroup[0]),
//                        Optional.empty(),
                        null)
                .saveAsTextFile("hdfs://localhost:8020/user/root/data/");
    }

    public static void main(String[] args) {
        mergeFile();
        System.out.println("merge finished!");
    }
}
