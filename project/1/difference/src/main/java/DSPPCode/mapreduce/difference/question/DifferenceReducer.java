package DSPPCode.mapreduce.difference.question;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

abstract public class DifferenceReducer extends Reducer<Text, Text, Text, NullWritable> {
  /**
   * TODO 请完成该抽象方法
   * -
   * 输出：输出两个输入文件的差集，即R-S
   * <p>
   * 输出格式与输入文件的格式相同
   */
    abstract public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException;
}
