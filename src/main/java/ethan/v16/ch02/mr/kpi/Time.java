package ethan.v16.ch02.mr.kpi;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

/**
 * @since 2014-9-20
 * @author ethan
 * 用户每小时PV的统计
 */

public class Time {

//	$time_local,1
//	$time_local,sum()

	public static class TimeMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{
		
		IntWritable one = new IntWritable(1);
		Text time_local = new Text();
		public void map(Object key, Text line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			MyKPI kpi = MyKPI.filterTime(line.toString());
			if(kpi.isValid()){
				try {
					String time_hour = kpi.getTime_local_Date_hour();
					time_local.set(time_hour);
					output.collect(time_local, one);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			}
			
		}
		
	}
	
	public static class TimeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			int sum =0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		String input = "hdfs://u1:9000/home/hadoop/data.v16/kpi*";
		String output = "hdfs://u1:9000/home/hadoop/out.v16";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("ETHAN.TIME.JOB");
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
		conf.setCombinerClass(TimeReducer.class); // 压缩？
		conf.setReducerClass(TimeReducer.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}
	
}
