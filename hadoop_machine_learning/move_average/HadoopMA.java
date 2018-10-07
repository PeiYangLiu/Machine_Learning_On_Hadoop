package hadoop_machine_learning.move_average;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMA {
    private static int window = 1;
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

    public static class MyReduce extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {
        public void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context)
                throws IOException, InterruptedException {
            int index = 1;
            double sum = 0.0;
            ArrayList<Double> queue = new ArrayList<>();
            for (TimeSeriesData val : values) {
                queue.add(val.getValue());
                sum = sum + val.getValue();
                if(queue.size()>window){
                    sum = sum - queue.remove(0);
                }
                if(index < window) {
                    context.write(new Text(key.getName() + " " + val.getDate()), new Text((sum / index) + ""));
                }
                else {
                    context.write(new Text(key.getName() + " " + val.getDate()), new Text((sum / window) + ""));
                }
                index = index + 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1] + "_pre");
        window = Integer.parseInt(args[2]);
        Configuration conf;
        Job job;
        conf = new Configuration();
        job = Job.getInstance(conf, "HadoopMA");
        job.setJarByClass(HadoopMA.class);
        job.setMapperClass(HadoopMA.MyMap.class);
        job.setReducerClass(HadoopMA.MyReduce.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesData.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);  //控制key相同的键值对分到同一个reducer，但将其打包发送的是GroupingComparator
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        //如果不加这个的话,由于默认的GroupingComparator比较的是复合key，故reducer收到的键值对是未打包的
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        if (!(job.waitForCompletion(true))) {
            System.exit(1);
        }
        System.out.println("pre finished");
        System.exit(0);
    }
}
