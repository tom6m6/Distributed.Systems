package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceMapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class DifferenceMapperImpl extends DifferenceMapper {
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    context.write(value,new Text(fileName));
  }
}