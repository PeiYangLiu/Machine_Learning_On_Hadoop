package hadoop_machine_learning.lr;

import java.util.regex.Pattern;


public class ParsePoint {
	private static final Pattern SPACE = Pattern.compile("\t");

    public DataPoint call(String line) {
      String[] tok = SPACE.split(line);
      double y = Double.parseDouble(tok[tok.length-1]);
      double[] x = new double[tok.length-1];
      for (int i = 0; i < tok.length-1; i++) {
        x[i] = Double.parseDouble(tok[i]);
      }
      return new DataPoint(x, y);
    }
    
    public double[] dcall(String line) {
        String[] tok = SPACE.split(line);
        double[] x = new double[PARAMENT.D];
        for (int i = 0; i < PARAMENT.D; i++) {
          x[i] = Double.parseDouble(tok[i]);
        }
        return x;
      }
}
