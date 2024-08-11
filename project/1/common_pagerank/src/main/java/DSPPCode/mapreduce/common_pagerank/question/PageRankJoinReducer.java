package DSPPCode.mapreduce.common_pagerank.question;

import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第一步的 Reducer
 */
public abstract class PageRankJoinReducer extends Reducer<Text, ReduceJoinWritable, Text, NullWritable> {
  /**
   * TODO 请完成该抽象方法
   *
   * <p>输出： 输出文本为网页链接关系和网页排名连接后的结果
   *
   * <p>如 A 1.0 B D 表示网页A的排名为1，并且链向网页B和D (题目中网页权重均按1.0计算)
   *
   * <p>可借助ReduceJoinWritable类来实现
   */
  @Override
  public abstract void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
      throws IOException, InterruptedException;
}
