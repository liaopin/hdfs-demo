import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceModel {

	//step 1:Mapper class
	public static class MapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable(1);
		
		@Override
		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//读取每一行,将Text转为String
			String lineValue = value.toString();
			//分割单词，以空格分割
			String[] strs = lineValue.split(" ");
			//分割后，将单词拿出组成key value,比如 <hadoop,1>
			for(String str : strs){
				mapOutputKey.set(str);
				context.write(mapOutputKey, mapOutputValue);
			}
		}
		
	}
	
	//step 2:Reducer class
	public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable(1);
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context arg2)
				throws IOException, InterruptedException {
			//tmp 
			int sum = 0;
			//对值进行相加
			for(IntWritable value : values){
				sum += value.get();
			}
			//set outputValue 
			outputValue.set(sum);
			//
			arg2.write(key, outputValue);
		}
		
	}
	
	//step 3:Dirver
	public int run(String[] args)throws Exception{
		//获取集群中的信息
		Configuration conf = new Configuration();
		
		//创建一个job任务
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//整个MapReduce程序入口，或者叫jar的入口,jar运行具体是哪个类
		job.setJarByClass(this.getClass());
		
		//设置job
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);
		Path outPutPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutPath);
		//设置Mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置Reducer
		job.setReducerClass(MapReduceReducer.class);
		
		//提交job --YARN
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		args = new String[] {
				"hdfs://bigdata-01.lp.com:8020/user/liaopin/mapreduce/input",
				"hdfs://bigdata-01.lp.com:8020/user/liaopin/mapreduce/output3" };
		
		//run job 
		int status = new MapReduceModel().run(args);
		
		//关闭
		System.exit(status);
	}

}
