package ethan.v16.ch02.mr.taobao.sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import ethan.v16.ch02.mr.kpi.PV;
import ethan.v16.ch02.util.CommonUtil;

/**
 * @since 2014-9-20
 * @author ethan 用户浏览器统计
 */

public class TaobaoSort {

	// $http_user_agent,1
	// $http_user_agent,sum()

	public static class TimeMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

		
		final IntWritable one = new IntWritable(1);
		final IntWritable num = new IntWritable();
		Text agent = new Text();
		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			num.set(Integer.parseInt(value.toString()));
//			MyKey outKey = new MyKey(key,num);
			output.collect(key, num);

		}

	}

	public static class BrowserReducer extends MapReduceBase implements Reducer<Text, IntWritable, MyKey, NullWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector< MyKey, NullWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			IntWritable second = new IntWritable();
			while(values.hasNext()){
				sum += values.next().get();
			}
			second.set(sum);
			MyKey mKey = new MyKey(key,second);
			output.collect(mKey, NullWritable.get());
//			output.collect(key, second);
		}

	}

	public static void main(String[] args) throws Exception {
		

		String input = "hdfs://u1:9000/home/hadoop/data.v16/taobao*";
		String output = "hdfs://u1:9000/home/hadoop/out.v16";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("ETHAN.BROWSER.JOB");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");

		// map 输出
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(MyKey.class);
		conf.setOutputValueClass(NullWritable.class);

		//自动把一行用“ ”把key和value分开
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.set("key.value.separator.in.input.line", " ");  
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(TimeMapper.class);
		conf.setCombinerClass(BrowserReducer.class); // 压缩？
		conf.setReducerClass(BrowserReducer.class);

//		conf.setPartitionerClass(MyPartitioner.class);
//		conf.setOutputValueGroupingComparator(MyGroupComparator.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		CommonUtil.deleteOutput(output, conf);
		JobClient.runJob(conf);
//		hdfs.copyToLocalFile(new Path(output), new Path("/vms/v16/aa"));
//		getFromHDFS(output, "/vms/v16/aa", conf);
		
		String libpath = System.getProperty("java.library.path");  
		System.out.println("libpath=" + libpath);  
		
		System.exit(0);
	}

	   public static boolean getFromHDFS(String src , String dst , Configuration conf){  
	        Path dstPath = new Path(dst) ;  
	        try{  
	            FileSystem dhfs = dstPath.getFileSystem(conf) ;  
	            dhfs.copyToLocalFile(false, new Path(src), dstPath) ;  
	        }catch(IOException ie){  
	            ie.printStackTrace() ;  
	            return false ;  
	        }  
	        return true ;  
	    }  

}
