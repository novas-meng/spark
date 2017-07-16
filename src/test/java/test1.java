import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mengfanshan on 2017/7/16.
 *     用java实现过滤器转化操作：
 */
public class test1 {
    //用java实现过滤器转化操作
    public static void fliter()
    {
        SparkConf sparkConf=new SparkConf().setAppName("novas");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        List<String> list=new ArrayList<String>();
        list.add("error a");
        list.add("error b");
        list.add("abc");
        JavaRDD<String> lines=sc.parallelize(list);

        JavaRDD<String> line1=sc.parallelize(list);
        lines=lines.union(line1);
        for (String str:lines.take(1))
        {
            System.out.println(str);
        }
    }
    //用java实现map转化操作
    public static void map()
    {
        SparkConf sparkConf=new SparkConf().setAppName("novas");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        ArrayList<Integer> list=new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        JavaRDD<Integer> rdd=sc.parallelize(list);
        JavaRDD<Integer> newrdd=rdd.map(new Function<Integer,Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer*integer;
            }
        });
        for (Integer i:newrdd.collect())
        {
            System.out.println(i);
        }

    }
    public static void main(final String[] args)
    {
        SparkConf sparkConf=new SparkConf().setAppName("novas");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd =sc.parallelize(Arrays.asList("hello world","hello you","world i love you"));

        JavaRDD<String> rdd_new=rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                ArrayList<String> list=new ArrayList<String>();
                String[] array=s.split(" ");
                for (int i=0;i<array.length;i++)
                {
                    list.add(array[i]);
                }
                return list.iterator();
            }
        });
        for (String str:rdd_new.collect())
        {
            System.out.println(str);
        }


    }
}
