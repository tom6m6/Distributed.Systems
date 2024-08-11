package DSPPCode.mapreduce.frequent_item_analysis.impl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }
    context.write(key, new IntWritable(count));
  }
}
