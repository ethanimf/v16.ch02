package ethan.v16.ch02.mr.kpi;

import java.io.IOException;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/**
 * @since 2014-9-20
 * @author ethan
 */

//$request,ip
//$request,sum(unique)

public class IP {

	
	public static class IPMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			MyKPI kpi = MyKPI.parserKPI(value.toString());
			if(kpi.isValid()){
				
				output.collect(new Text(kpi.getRequest()), new Text(kpi.getRemote_addr()));
			}
			
		}
		
	}
	
	public static class IPReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		Set<String> trees = new HashSet<String>(); //去重
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				
			//一定要在这里去重，因为就算在map去重，也是保证单节点已去重，不能保证节点在这里汇集后没有重复的。
			while(values.hasNext()){
				trees.add(values.next().toString());
			}
			//^^ 其实key不用你维护，values集合就是对应同一个key的
			output.collect(key, new Text(String.valueOf(trees.size())));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		String input = "hdfs://u1:9000/home/hadoop/data.v16/aaa.txt";
		String output = "hdfs://u1:9000/home/hadoop/out.v16";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("ETHAN.IP.JOB");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");

		// map 输出
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(IPMapper.class);
		conf.setCombinerClass(IPReducer.class); // 压缩？
		conf.setReducerClass(IPReducer.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}
}
