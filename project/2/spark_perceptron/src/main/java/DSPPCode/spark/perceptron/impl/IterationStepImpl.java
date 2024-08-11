package DSPPCode.spark.perceptron.impl;

import DSPPCode.spark.perceptron.question.IterationStep;
import DSPPCode.spark.perceptron.question.DataPoint;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;

public class IterationStepImpl extends IterationStep {

  @Override
  public Broadcast<double[]> createBroadcastVariable(JavaSparkContext sc, double[] localVariable) {
    return sc.broadcast(localVariable);
  }

  @Override
  public boolean termination(double[] old, double[] newWeightsAndBias) {
    double sum = 0;
    for (int i = 0; i < old.length; i++) {
      sum += Math.pow(old[i] - newWeightsAndBias[i], 2);
    }
    return sum < THRESHOLD;
  }

  @Override
  public double[] runStep(JavaRDD<DataPoint> points, Broadcast<double[]> broadcastWeightsAndBias) {
    double[] weightsAndBias = points.map(new ComputeGradient(broadcastWeightsAndBias.value()))
        .reduce(new VectorSum());
    for (int i = 0; i < weightsAndBias.length; i++) {
      weightsAndBias[i] = broadcastWeightsAndBias.value()[i] + STEP * weightsAndBias[i];
    }
    return weightsAndBias;
  }

  public static class VectorSum implements Function2<double[], double[], double[]> {
    @Override
    public double[] call(double[] a, double[] b) throws Exception {
      for (int i = 0; i < a.length; i++) {
        a[i] += b[i];
      }
      return a;
    }
  }

  public static class ComputeGradient implements Function<DataPoint, double[]> {
    public final double[] weightsAndBias;

    public ComputeGradient(double[] weightsAndBias) {
      this.weightsAndBias = weightsAndBias;
    }

    @Override
    public double[] call(DataPoint dataPoint) throws Exception {
      double[] gradient = new double[weightsAndBias.length];
      double dotProduct = 0;
      for (int i = 0; i < dataPoint.x.length; i++) {
        dotProduct += dataPoint.x[i] * weightsAndBias[i];
      }
      dotProduct += weightsAndBias[weightsAndBias.length - 1];
      if (dataPoint.y * dotProduct <= 0) {
        for (int i = 0; i < dataPoint.x.length; i++) {
          gradient[i] = dataPoint.y * dataPoint.x[i];
        }
        gradient[weightsAndBias.length - 1] = dataPoint.y;
      }
      return gradient;
    }
  }
}