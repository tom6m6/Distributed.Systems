package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinReducer;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class PageRankJoinReducerImpl extends PageRankJoinReducer {

  @Override
  public void reduce(Text key, Iterable<ReduceJoinWritable> values,
      Reducer<Text, ReduceJoinWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException {
    String[] row = new String[2];
    for(ReduceJoinWritable value : values){
      if(value.getTag().equals("2")){
        row[0] = value.getData();
      }
      else if(value.getTag().equals("1")){
        row[1] = value.getData();
      }
    }
    String line = key.toString() + " " + row[0] + " " + row[1];
    context.write(new Text(line), NullWritable.get());
  }
}

