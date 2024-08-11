package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;
public class DifferenceReducerImpl extends DifferenceReducer {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException{
    Iterator<Text> itr = values.iterator();
    boolean flagR = false;
    boolean flagS = false;
    while (itr.hasNext()) {
      String s = itr.next().toString();
      if (s.equals("R")) {
        flagR = true;
      }
      if (s.equals("S")) {
        flagS = true;
      }
    }
    if (flagR && !flagS) {
      context.write(key, NullWritable.get());
    }
  }
}