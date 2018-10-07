package hadoop_machine_learning.move_average;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData>{

    @Override
    public int getPartition(CompositeKey key, TimeSeriesData value, int numberOfPartitions) {
        return Math.abs((int) (hash(key.getName()) % numberOfPartitions));
    }

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // 质数
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
