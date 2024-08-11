package DSPPCode.mapreduce.transitive_closure.question;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

abstract public class TransitiveClosureMapper extends
    Mapper<Object, Text, Text, Text> {

  /**
   * TODO 请完成该方法
   */
  abstract public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException;
}



