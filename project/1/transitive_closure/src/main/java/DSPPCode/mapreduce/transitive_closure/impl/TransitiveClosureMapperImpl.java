package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureMapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;
public class TransitiveClosureMapperImpl extends TransitiveClosureMapper{
  private Text child = new Text();
  private Text parent = new Text();
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException{
    String[] v1 =value.toString().split("\\s+");
    String child = v1[0];
    String parent = v1[1];
    if (!child.equals("child")){
      context.write(new Text(child), new Text("-" + parent));
      context.write(new Text(parent), new Text("+" + child));
    }

  }

}
