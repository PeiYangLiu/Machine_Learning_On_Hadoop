package hadoop_machine_learning.kmeans;

import java.util.ArrayList;

public class Utils {


    public String pre(ArrayList<Point> centers, Point point) {
        double distance = Double.MAX_VALUE;
        Point result = centers.get(0);
        for (Point center : centers) {
            double tempDistance = getDistance(center, point);
            if (tempDistance < distance) {
                distance = tempDistance;
                result = center;
            }
        }
        return result.toString();
    }

    private double getDistance(Point a, Point b) {
        double distance = 0;
        for (int i = 0; i < a.getX().length; i++) {
            distance = distance + Math.pow(a.getX()[i] - b.getX()[i], 2.0);
        }
        return Math.pow(distance, 0.5);
    }
}
