package DSPPCode.mapreduce.frequent_item_analysis.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


abstract public class FrequentItemAnalysisReducer extends
    Reducer<Text, IntWritable, Text, NullWritable> {

  /**
   * TODO 请完成该抽象方法
   * -
   * 输出：
   * 满足支持度的n阶频繁项，其中每一个频繁项内部的物品需要按字典序升序排列
   */
  abstract public void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException;
}
