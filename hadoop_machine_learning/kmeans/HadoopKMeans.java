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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HadoopKMeans {
    private static ArrayList<Point> centers = new ArrayList<>();
    private static int k;

    public static class MyMap extends Mapper<Object, Text, Text, Text> {
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
                context.write(new Text(new Utils().pre(centers, point)), value);
            }
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        private ArrayList<Point> reduceCenters = new ArrayList<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int size = 0;
            for (Text val : values) {
                size = val.toString().trim().split("\t").length - 1;
                break;
            }
            double[] temp_X = new double[size];
            for (Text val : values) {
                String[] temp = val.toString().trim().split("\t");
                for (int i = 0; i < temp.length - 1; i++) {
                    temp_X[i] = temp_X[i] + Double.parseDouble(temp[i]);
                }
                context.write(val, key);
            }
            this.reduceCenters.add(new Point(temp_X, 0));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path centerPath = new Path(conf.get("center_path"));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(centerPath, true);
            try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), centerPath,
                    Text.class, IntWritable.class)) {
                final IntWritable value = new IntWritable(0);
                for (Point center : this.reduceCenters) {
                    out.append(new Text(center.toString()), value);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]);
        k = Integer.parseInt(args[2]);
        int iteration = Integer.parseInt(args[3]);
        Configuration conf;
        Job job;
        conf = new Configuration();
        InputStream in = null;
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader buff = null;
        in = hdfs.open(input);
        buff = new BufferedReader(new InputStreamReader(in));
        String ss = null;
        int index = 0;
        while ((ss = buff.readLine()) != null && index < k) {
            String[] temp = ss.split("\t");
            double[] temp_X = new double[temp.length - 1];
            int temp_y;
            for (int i = 0; i < temp.length - 1; i++) {
                temp_X[i] = Double.parseDouble(temp[i]);
            }
            temp_y = Integer.parseInt(temp[temp.length - 1]);
            centers.add(new Point(temp_X, temp_y));
            index++;
        }
        for (int i = 0; i < iteration; i++) {
            if (i > 0) {
                centers.clear();
                in = hdfs.open(new Path(args[1] + "/centers_" + (i - 1)));
                buff = new BufferedReader(new InputStreamReader(in));
                while ((ss = buff.readLine()) != null) {
                    String[] temp = ss.split(",");
                    double[] temp_X = new double[temp.length - 1];
                    for (int j = 0; j < temp.length - 1; j++) {
                        temp_X[j] = Double.parseDouble(temp[j]);
                    }
                    centers.add(new Point(temp_X, 0));
                }
            }
            Path output = new Path(args[1] + "_pre_" + i);
            conf.set("center_path", args[1] + "/centers_" + i);
            job = Job.getInstance(conf, "HadoopKMeans");
            job.setJarByClass(HadoopKMeans.class);
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            if (!(job.waitForCompletion(true))) {
                System.exit(1);
            }
            System.out.println("iteration " + i + " finished");
        }
        System.out.println("clustering finished");
        System.exit(0);
    }
}
