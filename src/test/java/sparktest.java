/**
 * Created by mengfanshan on 2017/7/14.
 */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
public class sparktest {
    /**
     * MIT.
     * Author: wangxiaolei(ÍõÐ¡À×).
     * Date:17-2-7.
     * Project:SparkJavaIdea.
     */


        public static void main(String[] args) {
            String logFile = "/Users/mengfanshan/Desktop/spark/README.md"; // Should be some file on your system
            SparkConf conf = new SparkConf().setAppName("Simple Application");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaRDD<String> logData = sc.textFile(logFile).cache();

            System.out.println(logData.partitions().get(0).toString());
            long numAs = logData.filter(new Function<String, Boolean>()
            {
                public Boolean call(String s)
                {
                    return s.contains("a");
                }
            }).count();

            long numBs = logData.filter(new Function<String, Boolean>() {
                public Boolean call(String s) { return s.contains("b"); }
            }).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

            sc.stop();
        }

}
