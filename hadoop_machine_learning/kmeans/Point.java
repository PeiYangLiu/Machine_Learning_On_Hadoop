package hadoop_machine_learning.kmeans;

public class Point {
    private double[] X;
    private int y;

    public Point(double[] X, int y){
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
}
