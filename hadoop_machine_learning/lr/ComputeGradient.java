package hadoop_machine_learning.lr;
public class ComputeGradient  {
    private final double[] weights;
    ComputeGradient(double[] weights) {
      this.weights = weights;
    }
    
    public static double dot(double[] a, double[] b) {
	    double x = 0;
	    for (int i = 0; i < PARAMENT.D; i++) {
	      x += a[i] * b[i];
	    }
	    return x;
	  }

    public double[] call(DataPoint p) {
      double[] gradient = new double[PARAMENT.D];
      for (int j = 0; j < PARAMENT.D; j++) {
        double dot = dot(weights, p.x);
        gradient[j] = (1 / (1 + Math.exp(-1 * dot)) - p.y) * p.x[j];
      }
      return gradient;
    }

    public int pre(DataPoint p){
        double dot = dot(weights, p.x);
        double prob = 1 / (1 + Math.exp(-dot));
        if(prob > 0.5){
            return 1;
        }
        return 0;
    }
  }