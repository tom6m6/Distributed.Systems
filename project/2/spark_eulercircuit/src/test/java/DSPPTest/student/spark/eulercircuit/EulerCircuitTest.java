package DSPPTest.student.spark.eulercircuit;

import DSPPCode.spark.eulercircuit.impl.EulerCircuitImpl;
/*
import DSPPCode.spark.progressive_students.impl.ProgressiveStudentsImpl;
import DSPPCode.spark.progressive_students.question.ProgressiveStudents;
 */
import DSPPTest.student.TestTemplate;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyList;

public class EulerCircuitTest extends TestTemplate {

  @Test
  public void test() {

    // 设置路径
    String inputPath = root + "/spark/eulercircuit/input";
    String outputPath = outputRoot + "/spark/eulercircuit";
    String outputFile = outputPath + "/part-00000";
    String answerFile = root + "/spark/eulercircuit/answer";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    String[] args = {inputPath, outputPath};
    EulerCircuitImpl eulerCircuit = new EulerCircuitImpl();
    eulerCircuit.run(args);

    // 检验结果
    verifyList(readFile2String(outputFile), readFile2String(answerFile));

    System.out.println("恭喜通过~");
  }
}
