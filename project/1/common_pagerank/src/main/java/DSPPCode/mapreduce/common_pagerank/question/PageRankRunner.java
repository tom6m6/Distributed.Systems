package DSPPCode.mapreduce.common_pagerank.question;

import DSPPCode.mapreduce.common_pagerank.impl.PageRankReducerImpl;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankRunner extends Configured implements Tool {

  // 最大的迭代次数
  public static final int MAX_ITERATION = 20;
  // 判定某页面 PageRank 收敛的差值阈值
  public static final double DELTA = 1e-6;
  // 从0开始记录当前迭代步数
  public static int iteration = 0;
  // 配置项中用于记录网页总数的键
  public static final String TOTAL_PAGE = "1";
  // 配置项中用于记录当前迭代步数的键
  public static final String ITERATION = "2";
  // 用于计数收敛节点数的 Counter 参数
  public static final String GROUP_NAME = "page_rank";
  public static final String COUNTER_NAME = "converge_page_count";

  @Override
  public int run(String[] args) throws Exception {

    int totalPage = Integer.parseInt(args[2]);
    getConf().setInt(PageRankRunner.TOTAL_PAGE, totalPage);
    getConf().setInt(PageRankRunner.ITERATION, iteration);

    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 设置程序的类名
    job.setJarByClass(getClass());

    // 设置数据的输入路径
    if (iteration == 0) {
      FileInputFormat.addInputPath(job, new Path(args[0]));
    } else {
      // 将上一次迭代的输出设置为输入
      FileInputFormat.addInputPath(job, new Path(args[1] + (iteration - 1)));
    }
    // 设置数据的输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1] + iteration));

    // 设置map方法及其输出键值对的数据类型
    job.setMapperClass(PageRankMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ReducePageRankWritable.class);

    // 设置reduce方法及其输出键值对的数据类型
    job.setReducerClass(PageRankReducerImpl.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    if (!job.waitForCompletion(true)
        || job.getCounters()
        .findCounter(GROUP_NAME, COUNTER_NAME)
        .getValue() == totalPage) {
      return -1;
    }
    return 0;
  }

  public int mainRun(String[] args) throws Exception {
    int exitCode = 0;
    while (iteration < MAX_ITERATION) {
      exitCode = ToolRunner.run(new PageRankRunner(), args);
      if (exitCode == -1) {
        break;
      }
      iteration++;
    }
    return exitCode;
  }
}
