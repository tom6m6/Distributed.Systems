package DSPPCode.mapreduce.frequent_item_analysis.question;

import DSPPCode.mapreduce.frequent_item_analysis.impl.FrequentItemAnalysisMapperImpl;
import DSPPCode.mapreduce.frequent_item_analysis.impl.FrequentItemAnalysisReducerImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.net.URISyntaxException;

abstract public class FrequentItemAnalysisRunner extends Configured implements Tool {
  private Job job;

  @Override
  public int run(String[] strings) throws Exception
  {
    job=Job.getInstance(getConf(),getClass().getSimpleName());
    job.setJarByClass(getClass());
    // 获取作业必要的参数，交易记录数,阶数,支持度

    int n = Integer.parseInt(strings[2]);
    double support = Double.parseDouble(strings[3]);
    int total = Integer.parseInt(strings[4]);
    configureMapReduceTask(job);
    // 设置数据的输入输出路径
    FileInputFormat.addInputPath(job, new Path(strings[0]));
    FileOutputFormat.setOutputPath(job, new Path(strings[1]));

    // 设置map方法的输出键值对数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // 设置reduce方法的输出键值对数据类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    //配置作业必要参数
    job.getConfiguration().setInt("count.of.transactions",total);
    job.getConfiguration().setInt("number.of.pairs",n);
    job.getConfiguration().setDouble("support",support);

    // 设置map和reduce方法
    job.setMapperClass(FrequentItemAnalysisMapperImpl.class);
    job.setReducerClass(FrequentItemAnalysisReducerImpl.class);

    return job.waitForCompletion(true)?0:1;
  }

  public Job getJob() throws IOException, URISyntaxException {
    return job;
  }
  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 1.Job类，用于配置MapReduce相关具体配置
   * 功能：
   * 配置任务运行相关信息。
   */
  abstract public void configureMapReduceTask(Job job)
      throws IOException, URISyntaxException;
}

