package hadoop_machine_learning.lr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 将lr的每一次迭代的矩阵梯度下降计算部分进行并行，即将每一行样本的误差分布计算，最后再进行reduce汇总。
 */
public class HadoopLR {
	public static double[] W;
	private static int input_size;
	public static class MyMap extends Mapper<Object,Text,IntWritable,DoubleWritable>{
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString().trim();
			if(null!=line){
				ParsePoint parsepoint =new ParsePoint();
				DataPoint point = parsepoint.call(line);
				double[] gredient = new ComputeGradient(W).call(point);
				for(int i=0;i<gredient.length;i++){
					context.write(new IntWritable(i+1),new DoubleWritable(gredient[i]));
				}
			}
		}

		/**
		 *  setup函数在执行map函数前执行一次，且只执行一次
		 */
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			String w_value = conf.get("W");
			int i=0;
			for(String e : w_value.trim().split(" ")){
				W[i++]=Double.parseDouble(e);
			}
		}
	}
	

	public static class MyReduce extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		
		private IntWritable ITERATOR = new IntWritable(PARAMENT.ITERATOR_NUM);
		public void reduce(IntWritable key,Iterable<DoubleWritable> values,Context context)
		throws IOException,InterruptedException{
			double gredient=0;
			for(DoubleWritable val:values){
				gredient += val.get();
			}
			gredient = gredient / input_size;
			W[key.get()-1] -= gredient;
			System.out.println("grad["+(key.get()-1)+"] is "+gredient);
			context.write(key,new DoubleWritable(W[key.get()-1]));
		}
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			String w_value = conf.get("W");
			int i=0;
			for(String e : w_value.trim().split(" ")){
				W[i++]=Double.parseDouble(e);
			}
		}
	}
	public static class MyCombine extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		public void reduce(IntWritable key,Iterable<DoubleWritable> values,Context context) 
		throws IOException,InterruptedException{
			double gredient=0;
			for(DoubleWritable val:values){
				gredient += val.get();
			}
			context.write(key,new DoubleWritable(gredient));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Path input = new Path(args[0]);
		Path output = new Path(args[1]+"_0");
		int ITERATOR= Integer.parseInt(args[2]);
		int weight_size = Integer.parseInt(args[3]);
		input_size = Integer.parseInt(args[4]);
		W = new double[weight_size];
		PARAMENT.D = weight_size;
		Configuration conf;
		Job job;
		conf = new Configuration();
	    String name ="W";
		for(int i =0;i<ITERATOR;i++){
			String value="";
			System.out.println("job: "+i+" start");
			if(0==i){
				for(int temp_x=0;temp_x<weight_size-1;temp_x++){
					value = value + "0 ";
				}
				value = value + "0";
			}
			else{
		          InputStream in=null;
		          FileSystem hdfs = FileSystem.get(conf);
		          BufferedReader buff =null;
		          Path path = new Path(args[1]+"_"+(i-1)+"/part-r-00000");
		          in=hdfs.open(path);
		          buff=new BufferedReader(new InputStreamReader(in));
		          String ss =null;
		          while((ss=buff.readLine())!=null){
		        	  int j=0;
		        	  while(ss.trim().charAt(j)>='0'&&ss.trim().charAt(j)<='9'){
		        		  j++;
		        	  }
		        	  while(ss.trim().charAt(j)<'0'||ss.trim().charAt(j)>'9'){
		        		  if(ss.trim().charAt(j)=='-')
		        			  break;
		        		  j++;
		        	  }
		        	  System.out.println("*************"+ss.substring(j));
		        	  value=value+ss.substring(j)+" ";
		          }
			}
			System.out.println("@@@@@@@@@@@@@@@@"+value);
			conf.set(name, value);
			job = Job.getInstance(conf, "HadoopLR"+"_"+i);
	        job.setJarByClass(HadoopLR.class);
	        job.setMapperClass(MyMap.class);
	        job.setCombinerClass(MyCombine.class);
	        job.setReducerClass(MyReduce.class);
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(DoubleWritable.class);
	        job.setOutputValueClass(DoubleWritable.class);
	        job.setOutputKeyClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, input);
	        FileOutputFormat.setOutputPath(job, output);
	        output = new Path(args[1]+"_"+(i+1));
	        if(!(job.waitForCompletion(true))){
	        	System.out.println("µü´ú"+i+"³ö´í");
	        	System.exit(1);
	        }
	        System.out.println(i+" finished");
		}
		System.exit(0);
        
 
	}
	
	

}
