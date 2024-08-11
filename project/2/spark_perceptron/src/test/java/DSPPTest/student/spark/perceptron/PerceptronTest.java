package DSPPTest.student.spark.perceptron;

import DSPPCode.spark.perceptron.question.PerceptronRunner;
import DSPPTest.student.TestTemplate;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class PerceptronTest extends TestTemplate {
  /**
   * 测试结果
   */
  @Test()
  public void testResult() throws Exception {
    String inputFile = root + "/spark/perceptron/input";
    String outputFile = root + "/spark/perceptron/output";
    String answerFile = root + "/spark/perceptron/answer";
    String[] args = new String[3];
    args[0] = inputFile;
    args[1] = outputFile;
    args[2] = "2";
    // 删除旧输出
    deleteFolder(outputFile);

    PerceptronRunner.run(args);

    verifyKV(readFile2String(outputFile), readFile2String(answerFile), 0.005);
    System.out.println("恭喜通过~");
  }
}