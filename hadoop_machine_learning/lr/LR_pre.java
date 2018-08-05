package hadoop_machine_learning.lr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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


public class LR_pre {
    public static double[] W;

    public static class PreMap extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (null != line) {
                ParsePoint parsepoint = new ParsePoint();
                DataPoint point = parsepoint.call(line);
                int pre = new ComputeGradient(W).pre(point);
                context.write(value, new IntWritable(pre));
            }
        }

        /**
         * setup函数在执行map函数前执行一次，且只执行一次
         */
        @Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            Configuration conf = context.getConfiguration();
            String w_value = conf.get("W");
            int i = 0;
            for (String e : w_value.trim().split(" ")) {
                W[i++] = Double.parseDouble(e);
            }
        }
    }


    public static class PreReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable ITERATOR = new IntWritable(PARAMENT.ITERATOR_NUM);

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable val : values) {
                result = val.get();
            }
            context.write(key, new IntWritable(result));
        }

        @Override
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            Configuration conf = context.getConfiguration();
            String w_value = conf.get("W");
            int i = 0;
            for (String e : w_value.trim().split(" ")) {
                W[i++] = Double.parseDouble(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path weight = new Path(args[1] + "/part-r-00000");
        Path output = new Path(args[2] + "_pre");
        int weight_size = Integer.parseInt(args[3]);
        W = new double[weight_size];
        PARAMENT.D = weight_size;
        Configuration conf;
        Job job;
        conf = new Configuration();
        String name = "W";
        String value = "";
        InputStream in = null;
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader buff = null;
        in = hdfs.open(weight);
        buff = new BufferedReader(new InputStreamReader(in));
        String ss = null;
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
            System.out.println("*************" + ss.substring(j));
            value = value + ss.substring(j) + " ";
        }
        System.out.println("@@@@@@@@@@@@@@@@" + value);
        conf.set(name, value);
        job = Job.getInstance(conf, "HadoopLR_pre");
        job.setJarByClass(LR_pre.class);
        job.setMapperClass(PreMap.class);
        job.setReducerClass(PreReduce.class);
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
