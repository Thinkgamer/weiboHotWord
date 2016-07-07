package hot_word;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class mainJob {
	
	public static final String  HDFS = "hdfs://127.0.0.1:9000";
    //	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	public static void main() throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated constructor stub
		Map<String,String>  path = new HashMap<String,String>();   //创建一个Map对象
		path.put("input", HDFS+"/mr/hot_word");
		path.put("output", HDFS+"/mr/hot_word/output");
		
		JobWorker.run(path);
		System.exit(0);
	}
}
