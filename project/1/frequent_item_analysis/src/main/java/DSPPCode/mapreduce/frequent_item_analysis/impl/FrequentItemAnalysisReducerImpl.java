package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class FrequentItemAnalysisReducerImpl extends FrequentItemAnalysisReducer {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }
    String nStr = context.getConfiguration().get("number.of.pairs");
    Integer n = Integer.valueOf(nStr);
    int length = key.toString().split(",").length;
    if(n != length)
      return;
    String countStr = context.getConfiguration().get("count.of.transactions");
    Integer total = Integer.valueOf(countStr);
    String supportStr = context.getConfiguration().get("support");
    Double support = Double.valueOf(supportStr);
    double curSupport = (double)count / total;
    if(curSupport >= support) {
      context.write(key, NullWritable.get());
    }
  }
}
