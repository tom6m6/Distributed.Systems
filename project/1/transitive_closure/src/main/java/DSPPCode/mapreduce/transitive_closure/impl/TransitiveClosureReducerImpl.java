package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureReducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
public class TransitiveClosureReducerImpl extends TransitiveClosureReducer{
  List<Text> grand = new ArrayList<Text>();
  List<Text> child = new ArrayList<Text>();
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException{
    for (Text v : values) {
      String s = v.toString();
      if (v.toString().startsWith("-")) {
        child.add(new Text(s.substring(1)));
      } else {
        grand.add(new Text(s.substring(1)));
      }
    }
    for (int i = 0; i < child.size(); i++) {
      for (int j = 0; j < grand.size(); j++) {
        context.write(grand.get(j),child.get(i) );
      }
    }
    grand.clear();
    child.clear();
  }

}
