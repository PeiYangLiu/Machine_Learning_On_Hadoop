package hadoop_machine_learning.kmeans;

import java.util.Arrays;

public class Point {
    private double[] X;
    private int y;

    public Point(double[] X, int y) {
        this.X = X;
        this.y = y;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public double[] getX() {
        return X;
    }

    public void setX(double[] x) {
        X = x;
    }

    @Override
    public String toString() {
        String result = "";
        for (double x : X
        ) {
            result = result + x + ",";
        }
        return result;
    }
}
