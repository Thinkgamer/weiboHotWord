package hot_word;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class mainJob {
	
	public static final String  HDFS = "hdfs://127.0.0.1:9000";
    //	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated constructor stub
		Map<String,String>  path = new HashMap<String,String>();   //创建一个Map对象
		path.put("input", HDFS+"/mr/hot_word/file");
		path.put("output", HDFS+"/mr/hot_word/output");
		
		JobWorker.run(path); //针对输入的key_word进行统计
		System.exit(0);
	}
}
	