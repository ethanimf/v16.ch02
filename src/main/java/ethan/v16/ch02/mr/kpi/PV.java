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
 * @author ethan
 */

//$request,1
//$request,sum()

public class PV {

	// ^^ mapper和reducer的类型必须是public static 否则初始化会报错
	public static class PVMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

		Text word = new Text();
		final IntWritable one = new IntWritable(1);

		public void map(Object key, Text line, OutputCollector<Text, IntWritable> output, Reporter report) throws IOException {
//			System.out.println(line.toString());
			MyKPI kpi = MyKPI.filterPVs(line.toString());
			if (kpi.isValid) {
				word.set(kpi.getRequest());
				output.collect(word, one);
			}
		}
	}

	public static class PVReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// ^^ 注意实现map和reduce的时候，参数是不一样的
		IntWritable sums = new IntWritable();

		public void reduce(Text request, Iterator<IntWritable> ones, OutputCollector<Text, IntWritable> output, Reporter report) throws IOException {

			// 很多的map在这里集合
			int sum = 0;
			// sum += ones;
			while (ones.hasNext()) {
				sum += ones.next().get();
			}
			sums.set(sum);
			output.collect(request, sums);
		}
	}
	

	public static void main(String[] args) throws Exception {
		String input = "hdfs://u1:9000/home/hadoop/data.v16.ch02/access*";
		String output = "hdfs://u1:9000/home/hadoop/out.v16.ch02";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("ETHAN.PV.JOB");
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

		conf.setMapperClass(PVMapper.class);
		conf.setCombinerClass(PVReducer.class); // 压缩？
		conf.setReducerClass(PVReducer.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		CommonUtil.deleteOutput(output, conf);
		JobClient.runJob(conf);
		System.exit(0);
	}
	

}
