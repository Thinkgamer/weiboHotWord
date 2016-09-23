package org.apache.mr.hotword;

/**
 * Created by thinkgamer on 16-9-22.
 */
import javax.swing.JPanel;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.util.Map;
import java.util.TreeMap;

public class LineCharts extends ApplicationFrame {
    public LineCharts(String s) {
        super(s);
        setContentPane(createDemoLine());
    }
    public static void run() {
        LineCharts fjc = new LineCharts("热词走势图");
        fjc.pack();
        RefineryUtilities.centerFrameOnScreen(fjc);
        fjc.setVisible(true);
    }
    // 生成显示图表的面板
    public static JPanel createDemoLine() {
        JFreeChart jfreechart = createChart(createDataset());
        return new ChartPanel(jfreechart);
    }
    // 生成图表主对象JFreeChart
    public static JFreeChart createChart(DefaultCategoryDataset linedataset) {
        //定义图表对象
        JFreeChart chart = ChartFactory.createLineChart("热词走势图", // chart title
                "时间", // domain axis label
                "数量", // range axis label
                linedataset, // data
                PlotOrientation.VERTICAL, // orientation
                true, // include legend
                true, // tooltips
                false // urls
        );
        CategoryPlot plot = chart.getCategoryPlot();
        // customise the range axis...
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        rangeAxis.setAutoRangeIncludesZero(true);
        rangeAxis.setUpperMargin(0.20);
        rangeAxis.setLabelAngle(Math.PI / 2.0);
        return chart;
    }
    //生成数据
    public static DefaultCategoryDataset createDataset() {
        //从Hive中读取数据
        Map<String, String> map = new TreeMap<String, String>();
        map = inHive.getResult();
        for(int i=1;i<13;i++){
            if(map.get(Integer.toString(i))==null){
                map.put(Integer.toString(i),"0");
            }
        }

        DefaultCategoryDataset linedataset = new DefaultCategoryDataset();
        //  各曲线名称
        String series1 = "热词";
        //    横轴名称(列名称)

        String type1 = "1:00";
        String type2 = "2:00";
        String type3 = "3:00";
        String type4 = "4:00";
        String type5 = "5:00";
        String type6 = "6:00";
        String type7 = "7:00";
        String type8 = "8:00";
        String type9 = "9:00";
        String type10 = "10:00";
        String type11 = "11:00";
        String type12 = "12:00";
        linedataset.addValue(Integer.parseInt(map.get("1")), series1, type1);
        linedataset.addValue(Integer.parseInt(map.get("2")), series1, type2);
        linedataset.addValue(Integer.parseInt(map.get("3")), series1, type3);
        linedataset.addValue(Integer.parseInt(map.get("4")), series1, type4);
        linedataset.addValue(Integer.parseInt(map.get("5")), series1, type5);
        linedataset.addValue(Integer.parseInt(map.get("6")), series1, type6);
        linedataset.addValue(Integer.parseInt(map.get("7")), series1, type7);
        linedataset.addValue(Integer.parseInt(map.get("8")), series1, type8);
        linedataset.addValue(Integer.parseInt(map.get("9")), series1, type9);
        linedataset.addValue(Integer.parseInt(map.get("10")), series1, type10);
        linedataset.addValue(Integer.parseInt(map.get("11")), series1, type11);
        linedataset.addValue(Integer.parseInt(map.get("12")), series1, type12);
        return linedataset;
    }
}