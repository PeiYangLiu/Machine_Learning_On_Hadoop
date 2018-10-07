package hadoop_machine_learning.move_average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMA {
    public static class MyMap extends Mapper<Object, Text, CompositeKey, IntWritable> {
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
                context.write(value, new IntWritable(0));
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
        Path input = new Path(args[1]);
        Path output = new Path(args[2] + "_pre");
        Configuration conf;
        Job job;
        conf = new Configuration();
        job = Job.getInstance(conf, "HadoopKNN");
        job.setJarByClass(HadoopMA.class);
        job.setMapperClass(HadoopMA.MyMap.class);
        job.setReducerClass(HadoopMA.MyReduce.class);
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
