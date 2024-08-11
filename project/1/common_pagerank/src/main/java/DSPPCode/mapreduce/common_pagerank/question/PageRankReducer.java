package DSPPCode.mapreduce.common_pagerank.question;

import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第二步的 Reducer
 */
public abstract class PageRankReducer extends Reducer<Text, ReducePageRankWritable, Text, NullWritable> {
  /**
   * TODO 请完成该抽象方法
   *
   * <p>输出： 网页的链接关系和最终的排名值
   *
   * <p>可借助ReducePageRankWritable类来实现
   */
  @Override
  public abstract void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException;
}
