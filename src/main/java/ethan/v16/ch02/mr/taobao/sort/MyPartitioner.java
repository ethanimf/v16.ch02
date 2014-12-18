package ethan.v16.ch02.mr.taobao.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * first相同的key放在同一个reduce处理
 * @since 2014-9-23
 * @author ethan
 */

public class MyPartitioner implements Partitioner<MyKey, NullWritable>{

	public void configure(JobConf job) {
	}

	public int getPartition(MyKey key, NullWritable value, int numPartitions) {
		
//		return 0;
//		
//		 return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		return (key.first.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
