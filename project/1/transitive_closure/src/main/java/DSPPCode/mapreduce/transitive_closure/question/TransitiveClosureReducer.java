package DSPPCode.mapreduce.transitive_closure.question;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

abstract public class TransitiveClosureReducer extends
    Reducer<Text, Text, Text, Text> {

  /**
   * TODO 请完成该方法
   */

  abstract public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException;
}
