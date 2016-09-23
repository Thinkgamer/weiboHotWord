package org.apache.mr.hotword;

/**
 * Created by thinkgamer on 16-9-22.
 */
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
        path.put("input", HDFS+"/file/weibo");
        path.put("output", HDFS+"/file/weibo/output");

        JobWorker.run(path); //针对输入的key_word进行统计
        inHive.run(path);     //调用inHive的run方法，将运行的结果写入hive表
        LineCharts.run();     //将计算的结果以折线的形式画出
    }
}