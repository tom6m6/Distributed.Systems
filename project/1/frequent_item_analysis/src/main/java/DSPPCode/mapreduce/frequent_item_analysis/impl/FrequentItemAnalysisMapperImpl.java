package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisMapper;
import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FrequentItemAnalysisMapperImpl extends FrequentItemAnalysisMapper {

  private void subSet(List<String> strs, List<String> path, int i, Mapper<LongWritable, Text, Text, IntWritable>.Context context, int m)
      throws IOException, InterruptedException {
    if(i == strs.size() || path.size() == m) {
      if(path.size() != 0) {
        StringBuilder builder = new StringBuilder();
        for (String s : path) {
          builder.append(s);
          builder.append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        context.write(new Text(builder.toString()), new IntWritable(1));
      }
      return;
    }
    path.add(strs.get(i));
    subSet(strs, path, i + 1, context, m);
    path.remove(path.size() - 1);
    subSet(strs, path, i + 1, context, m);
  }

  @Override
  public void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    String s = value.toString();
    String[] split = s.split(",");
    List<String> list = new ArrayList<>();
    for (String s1 : split) {
      list.add(s1);
    }
    list = new SortHelperImpl().sortSeq(list);
    int m = context.getConfiguration().getInt("number.of.pairs", 0);
    subSet(list, new ArrayList<String>(), 0, context, m);
  }
}
