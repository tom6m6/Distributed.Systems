package DSPPTest.student.mapreduce.transitive_closure;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyList;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureRunner;
import DSPPTest.student.TestTemplate;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class TransitiveClosureTest extends TestTemplate {

  @Test
  public void test() throws Exception {

    //设置路径
    String inputPath = root + "/mapreduce/transitive_closure/input";
    String outputPath = outputRoot + "/mapreduce/transitive_closure";
    String outputFile = outputPath + "/part-r-00000";
    String answerFile = root + "/mapreduce/transitive_closure/answer";

    //删除旧输出
    deleteFolder(outputPath);

    //执行
    String[] args = {inputPath, outputPath};
    ToolRunner.run(new TransitiveClosureRunner(), args);
    System.out.println(outputFile);
    System.out.println(answerFile);

    //检验结果
    verifyList(readFile2String(outputFile), readFile2String(answerFile));

    System.out.println("恭喜通过~");
  }
}
