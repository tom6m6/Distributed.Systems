package DSPPCode.spark.warm_up.impl;

import DSPPCode.spark.warm_up.question.WordCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Java 答案示例
 */
public class WordCountImpl extends WordCount {

  @Override
  public JavaPairRDD<String, Integer> wordCount(JavaRDD<String> lines) {
    // 将每行内容按分隔符拆分成单个单词
    JavaRDD<String> words = lines.flatMap((String line)
        -> Arrays.asList(line.split(" ")).iterator());
    // 将每个单词的频数设置为1，即将每个单词映射为[单词, 1]
    JavaPairRDD<String, Integer> wordPairs = words
        .mapToPair((String word) -> new Tuple2<>(word, 1));
    // 返回累加计数结果
    return wordPairs.reduceByKey(Integer::sum);
  }

}
