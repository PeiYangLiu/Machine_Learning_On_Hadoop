package hadoop_machine_learning.lr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PARAMENT {
    public static int D = 21;
    public static int ITERATOR_NUM = 1;

    public static double[] call(double[] a, double[] b) {
        double[] result = new double[D];
        for (int j = 0; j < D; j++) {
            result[j] = a[j] + b[j];
        }
        return result;
    }

    public static double[] outPutW(String s_path, Configuration conf) throws IOException {
        double[] w = new double[10];
        InputStream in = null;
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader buff = null;
        Path path = new Path(s_path + "/part-r-00000");
        in = hdfs.open(path);
        buff = new BufferedReader(new InputStreamReader(in));
        String ss = null;
        int k = 0;
        while ((ss = buff.readLine()) != null) {
            int j = 0;
            while (ss.trim().charAt(j) >= '0' && ss.trim().charAt(j) <= '9') {
                j++;
            }
            while (ss.trim().charAt(j) < '0' || ss.trim().charAt(j) > '9') {
                if (ss.trim().charAt(j) == '-')
                    break;
                j++;
            }
            //System.out.println("*************"+ss.substring(j));
            w[k] = Double.parseDouble(ss.substring(j));
            k++;
        }
        return w;
    }
}
