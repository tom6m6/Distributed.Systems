package DSPPCode.spark.warm_up.question;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class WordCount {

  private static final String MODE = "local";

  public void run(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(MODE, getClass().getName());
    // 读入文本数据，创建名为lines的RDD
    JavaRDD<String> lines = sc.textFile(args[0]);
    JavaPairRDD<String, Integer> counter = wordCount(lines);
    // 输出计数结果到文本文件中
    counter.saveAsTextFile(args[1]);
    sc.close();
  }

  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中完成词频统计的功能
   */
  public abstract JavaPairRDD<String, Integer> wordCount(JavaRDD<String> lines);
}
