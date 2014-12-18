package ethan.v16.ch02.mr.kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import ethan.v16.ch02.util.CommonUtil;

/**
 * @since 2014-9-20
 * @author ethan 用户浏览器统计
 */

public class Browser {

	// $http_user_agent,1
	// $http_user_agent,sum()

	public static class TimeMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

		IntWritable one = new IntWritable(1);
		Text agent = new Text();

		public void map(Object key, Text line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			MyKPI kpi = MyKPI.filterBrowser(line.toString());
			if (kpi.isValid()) {
				agent.set(kpi.getHttp_user_agent());
				output.collect(agent, one);
			}

		}

	}

	public static class BrowserReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://u1:9000/home/hadoop/data.v16.ch02/access.*.log";
		String output = "hdfs://u1:9000/home/hadoop/out.v16.ch02";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("ETHAN.BROWSER.JOB");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");

		// map 输出
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(TimeMapper.class);
		conf.setCombinerClass(BrowserReducer.class); // 压缩？
		conf.setReducerClass(BrowserReducer.class);

//		conf.setPartitionerClass(FirstPartitioner.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
//		CommonUtil.deleteOutput(output, conf);
		JobClient.runJob(conf);
		System.exit(0);
	}


}
