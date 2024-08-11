package DSPPCode.spark.warm_up.impl

import DSPPCode.spark.warm_up.question.WordCount
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

/**
 * Scala 答案示例
 * 注意: 此处为避免与 Java 示例冲突, 临时将类名改为 XXXScalaImpl. 学生使用 Scala 做题时切勿如此命名
 */
class WordCountScalaImpl extends WordCount {

  override def wordCount(lines: JavaRDD[String]): JavaPairRDD[String, Integer] = {
    // 将每行内容按分隔符拆分成单个单词
    val words = lines.rdd.flatMap(line => line.split(" "))
    // 将每个单词的频数设置为1，即将每个单词映射为[单词, 1]
    val wordPairs = words.map(word => (word, new Integer(1)))
    // 累加计数
    val result = wordPairs.reduceByKey(_ + _)
    JavaPairRDD.fromRDD(result)
  }

}
