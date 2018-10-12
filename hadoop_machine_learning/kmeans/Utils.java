package hadoop_machine_learning.kmeans;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class Utils {
    private class Result {
        int y;
        double distance;

        Result(int y, double distance) {
            this.distance = distance;
            this.y = y;
        }
    }

    public int pre(ArrayList<Point> train_set, Point point, int k) {
        PriorityQueue<Result> results = new PriorityQueue<>(resultComparator);
        for (Iterator<Point> it2 = train_set.iterator(); it2.hasNext(); ) {
            Point temp_point = it2.next();
            double distance = 0.0;
            for (int i = 0; i < temp_point.getX().length; i++) {
                distance = distance + Math.pow(temp_point.getX()[i] - point.getX()[i], 2.0);
            }
            distance = Math.pow(distance, 0.5);
            results.add(new Result(temp_point.getY(), distance));
        }
        int[] result_list = new int[10];
        int max_size = 0;
        int result = 0;
        int index = 0;
        while (!results.isEmpty()) {
            index = index + 1;
            if(index > k){
                break;
            }
            Result temp = results.poll();
            result_list[temp.y] = result_list[temp.y] + 1;
            if (max_size < result_list[temp.y]) {
                max_size = result_list[temp.y];
                result = temp.y;
            }
        }
        return result;
    }

    private static Comparator<Result> resultComparator = new Comparator<Result>() {
        @Override
        public int compare(Result c1, Result c2) {
            if ((c1.distance - c2.distance) > 0) {
                return 1;
            }
            else if ((c1.distance - c2.distance) == 0) {
                return 0;
            }
            return -1;
        }
    };
}
