package hadoop_machine_learning.lr.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class HadoopLR_ok {
	//�̳�Mapper�ӿڣ�����map����������Ϊ<Object,Text>
	//�������Ϊ<Text,LongWritable>
	public static class MyMap extends Mapper<Object,Text,IntWritable,Text>{
		//��¼����ʱ��
		private IntWritable iterator =new IntWritable(PARAMENT.ITERATOR_NUM); 
		//��дmap����
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString().trim();
			//��õ�
			ParsePoint parsepoint =new ParsePoint();
			DataPoint point = parsepoint.call(line);
			//�õ�ÿ������w�Ĺ�ϵ
			double[] gredient = new ComputeGradient(PARAMENT.W).call(point);
			System.out.println(gredient);
			String str ="";
			//DoubleWritable[] D_gredient=new DoubleWritable[gredient.length];
			for(int i=0;i<gredient.length;i++){
				//D_gredient[i].set(gredient[i]);
				str =str+ gredient[i]+" ";
			}
			System.out.println(gredient);
			//ArrayWritable a = new ArrayWritable(DoubleWritable.class);
			//a.set(D_gredient);
			//���Ϊ��������ݶ�����
			Text out = new Text(str);
			context.write(iterator,out);
		}
	}
	
	//�̳�reduce�����࣬����reduce����������Ϊ<Text,long>
	//�������Ϊ<Text,long>
	
	public static class MyReduce extends Reducer<IntWritable,Text,IntWritable,Text>{
		
		private IntWritable ITERATOR = new IntWritable(PARAMENT.ITERATOR_NUM);
		//private Text word_number =0;
		public void reduce(IntWritable key,Iterable<Text> values,Context context) 
		throws IOException,InterruptedException{
			List<double[]> gredient_list  = new ArrayList<double[]>();
			ParsePoint parsepoint =new ParsePoint();
			for(Text val:values){
				double[] gredient = parsepoint.dcall(val.toString().trim());
				gredient_list.add(gredient);
				System.out.println(gredient);
			}
			double[] gradient = gredient_list.get(0);
		      for(int k =1;k<gredient_list.size();k++){
		    	  gradient = PARAMENT.call(gradient, gredient_list.get(k));
		      }
		  
		      for (int j = 0; j < PARAMENT.D; j++) {
		        PARAMENT.W[j] -= gradient[j];
		      }
			String w_str = "";
			for(int j = 0; j < PARAMENT.D; j++){
				w_str =w_str+PARAMENT.W[j]+" ";
			}
			Text output = new Text(w_str);
			//�ռ����
			context.write(ITERATOR,output);
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HadoopLR");
        job.setJarByClass(HadoopLR_ok.class);
        job.setMapperClass(MyMap.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        
        
 
	}
	
	

}
