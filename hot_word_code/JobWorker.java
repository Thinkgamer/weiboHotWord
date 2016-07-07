package hot_word;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobWorker {

	static String filename;
	
	public static class hot_word_Map extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			filename = ((FileSplit) inputsplit).getPath().getName();
			System.out.println("filename ：" + filename);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}		
	}
	
	public static class hot_word_Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String input = path.get("input");
		String output = path.get("output");
		
		Job job = new Job(new Configuration(), "hot_word");
		job.setJarByClass(JobWorker.class);
		
		//1：设置input路径
		FileInputFormat.addInputPath(job, new Path(input));
		//指定如何对文件进行格式化
		job.setInputFormatClass(TextInputFormat.class);
		
		//2：指定自定义的Map类
		job.setMapperClass(hot_word_Map.class);
		//指定key，value输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//3：分区
		//设置reduce个数
		job.setNumReduceTasks(1);
		
		//4：TODO排序分组
		
		//5：归约
		
		//6：指定自定义的reduce类
		job.setReducerClass(hot_word_Reduce.class);
		//指定reduce输出key，value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//7：指定输出文件格式
		FileOutputFormat.setOutputPath(job, new Path(output));
		//指定输出文件格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8：提交作业
		job.waitForCompletion(true);
	}

}
