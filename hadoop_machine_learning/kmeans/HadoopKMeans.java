package hadoop_machine_learning.kmeans;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HadoopKMeans {
    private static ArrayList<Point> train_set = new ArrayList<>();
    private static int k;
    public static class MyMap extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (null != line) {
                String[] temp = line.split("\t");
                double[] temp_X = new double[temp.length - 1];
                int temp_y;
                for (int i = 0; i < temp.length - 1; i++) {
                    temp_X[i] = Double.parseDouble(temp[i]);
                }
                temp_y = Integer.parseInt(temp[temp.length - 1]);
                Point point = new Point(temp_X, temp_y);
                context.write(value, new IntWritable(new Utils().pre(train_set, point, k)));
            }
        }
    }

    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable val : values) {
                result = val.get();
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Path train = new Path(args[0]);
        Path input = new Path(args[1]);
        Path output = new Path(args[2] + "_pre");
        k = Integer.parseInt(args[3]);
        Configuration conf;
        Job job;
        conf = new Configuration();
        InputStream in = null;
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader buff = null;
        in = hdfs.open(train);
        buff = new BufferedReader(new InputStreamReader(in));
        String ss = null;
        while ((ss = buff.readLine()) != null) {
            String[] temp = ss.split("\t");
            double[] temp_X = new double[temp.length - 1];
            int temp_y;
            for (int i = 0; i < temp.length - 1; i++) {
                temp_X[i] = Double.parseDouble(temp[i]);
            }
            temp_y = Integer.parseInt(temp[temp.length - 1]);
            train_set.add(new Point(temp_X, temp_y));
        }
        job = Job.getInstance(conf, "HadoopKNN");
        job.setJarByClass(HadoopKMeans.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        if (!(job.waitForCompletion(true))) {
            System.exit(1);
        }
        System.out.println("pre finished");
        System.exit(0);
    }
}
