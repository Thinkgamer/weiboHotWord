package hot_word;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
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
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class JobWorker {

	static String filename,day,hour;
	static int flag = 5;    //阀值，决定是否为高频词
	
	public static class hot_word_Map extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private static Text map_key = new Text();
		private static LongWritable map_value = new LongWritable(1);
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			filename = ((FileSplit) inputsplit).getPath().getName();	
			//由filename得到day和hour
			day = filename.substring(0, 8);
			hour = filename.substring(8, 10);
			System.out.println("filename ：" + filename);
			System.out.println("Day ：" + day);
			System.out.println("Hour ：" + hour);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			//分词
			StringReader sr=new StringReader(line);  
	        IKSegmenter ik=new IKSegmenter(sr, true);  
	        Lexeme lex=null;  
	        while((lex=ik.next())!=null){  
	        	map_key.set(day+"," + hour + "," + lex.getLexemeText());
	            context.write(map_key,map_value);
	        }  
		}	
	}
	
	public static class hot_word_Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (LongWritable longWritable : values) {
				sum += longWritable.get();
			}
			if(sum>=flag){
				context.write(key,new LongWritable(sum));
			}
		}
		
	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		String input = path.get("input");
		String output = path.get("output");
		
		//如果输出路径存在的话就删除
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rm(output);
		
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
		job.setMapOutputValueClass(LongWritable.class);
		
		//3：分区
		//设置reduce个数
		job.setNumReduceTasks(1);
		
		//4：TODO排序分组
		
		//5：归约
		
		//6：指定自定义的reduce类
		job.setReducerClass(hot_word_Reduce.class);
		//指定reduce输出key，value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//7：指定输出文件格式
		FileOutputFormat.setOutputPath(job, new Path(output));
		//指定输出文件格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//8：提交作业
		job.waitForCompletion(true);
	}

}
