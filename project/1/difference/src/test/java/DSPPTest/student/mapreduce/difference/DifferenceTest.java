package DSPPTest.student.mapreduce.difference;

import DSPPCode.mapreduce.difference.impl.DifferenceMapperImpl;
import DSPPCode.mapreduce.difference.impl.DifferenceReducerImpl;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class DifferenceTest extends TestTemplate {

    @Test
    public void test() throws Exception {
        //set dir
        String inputFolder = root + "/mapreduce/difference/input";
        String outputFolder = outputRoot + "/mapreduce/difference";
        String outputFile = outputFolder + "/part-r-00000";
        String answerFile = root + "/mapreduce/difference/answer";

        //delete old dirl
        deleteFolder(outputFolder);

        //do
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(DifferenceTest.class);
        job.setMapperClass(DifferenceMapperImpl.class);
        job.setReducerClass(DifferenceReducerImpl.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        job.waitForCompletion(false);

        //check result
        verifyKV(readFile2String(outputFile), readFile2String(answerFile), new KVParser("\t"));
    }
}
