package DSPPTest.student.mapreduce.frequent_item_analysis;

import DSPPCode.mapreduce.frequent_item_analysis.impl.FrequentItemAnalysisRunnerImpl;
import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisRunner;
import DSPPTest.student.TestTemplate;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.FileOperator.readFolder2StringExcludeHiddenFiles;
import static DSPPTest.util.Verifier.verifyList;

public class FrequentItemAnalysisTest extends TestTemplate {
  @Test
  public void test() throws Exception {
    String inputPath = root + "/mapreduce/frequent_item_analysis/input";
    String outputPath = outputRoot + "/mapreduce/frequent_item_analysis";
    String outputSuccessFile = outputPath + "/_SUCCESS";
    String answerFile = root + "/mapreduce/frequent_item_analysis/answer";

    deleteFolder(outputPath);
    FrequentItemAnalysisRunner frequentItemAnalysisRunner = new FrequentItemAnalysisRunnerImpl();
    String [] args = {inputPath,outputPath,String.valueOf(2),String.valueOf(0.5),String.valueOf(7)};
    int exitCode = ToolRunner.run(frequentItemAnalysisRunner,args);
    File successFile = new File(outputSuccessFile);
    if (!successFile.delete()){
      System.err.println("不能删除Success文件");
    }
    verifyList(readFolder2StringExcludeHiddenFiles(outputPath), readFile2String(answerFile));
    System.out.println("恭喜通过~");
    System.exit(exitCode);
  }
}
