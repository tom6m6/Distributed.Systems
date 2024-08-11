package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.common_pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {

  @Override
  public void reduce(Text key, Iterable<ReducePageRankWritable> values,
      Reducer<Text, ReducePageRankWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException {
    double pr = 0.0;
    String outPage = "";
    double lastPr = 0.0;
    for(ReducePageRankWritable value : values){
      if(value.getTag().equals(ReducePageRankWritable.PR_L)){
        try {
          pr += Double.parseDouble(value.getData());
        } catch (NumberFormatException e){
          e.printStackTrace();
        }
      }
      else{
        String[] segment = value.getData().split(" ", 3);
        outPage += segment[2];
        lastPr = Double.parseDouble(segment[1]);
      }
    }
    int totalPage = context.getConfiguration().getInt("1", 0);
    pr = 0.85 * pr + (1 - 0.85) / totalPage;
    if(Math.abs(pr - lastPr) < PageRankRunner.DELTA){
      context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME).increment(1L);
    }
    String out = key.toString() + " " + String.valueOf(pr) + " " + outPage;
    context.write(new Text(out), NullWritable.get());
  }
}

