package hadoop_machine_learning.move_average;

import java.io.IOException;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
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
    public static class MyMap extends Mapper<Object, Text, CompositeKey, TimeSeriesData> {
        private final CompositeKey reducerKey = new CompositeKey();
        private final TimeSeriesData reducerValue = new TimeSeriesData();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            //  空数据处理
            if ((line == null) || (line.length() == 0)) {
                return;
            }
            String[] records = StringUtils.split(line, ",");
            if (records.length == 3) {
                Date date = DateUtil.getDate(records[1]);
                if (date == null) {
                    return;
                }
                long timestamp = date.getTime();
                reducerKey.setName(records[0]);
                reducerKey.setTimestamp(timestamp);
                reducerValue.setTimestamp(timestamp);
                reducerValue.setValue(Double.parseDouble(records[2]));
                context.write(reducerKey, reducerValue);
            }
        }
    }

    public static class MyReduce extends Reducer<CompositeKey, TimeSeriesData, Text, IntWritable> {
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
