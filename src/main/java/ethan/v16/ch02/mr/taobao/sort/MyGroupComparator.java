package ethan.v16.ch02.mr.taobao.sort;

import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * first相同的key归为一个group
 * @since 2014-9-23
 * @author ethan
 */

//不使用分组的效果
//Taobao|67
//Taobao|52
//Taobao|31

//使用分组的效果
//Taobao|67
//Taobao|67
//Taobao|67
public class MyGroupComparator extends WritableComparator {

	/**
	 * @param keyClass
	 */
	// ^^父类没有无参构造方法，所以子类一定要定义一个构造方法，调用父类的有参构造
	protected MyGroupComparator() {
		super(MyKey.class, true);
	}

	//^^ w1,w2 是map输出的key
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
//		return super.compare(a, b);
/*		MyKey k1 = (MyKey)w1;
		MyKey k2 = (MyKey)w2;
//		first相同的key归为一个group
		return k1.first.compareTo(k2.first);*/
		
		Integer iw1 = Integer.parseInt(w1.toString());
		Integer iw2 = Integer.parseInt(w2.toString());
//		IntWritable iw2 = (IntWritable) w2;
		return -iw1.compareTo(iw2);
	}

}
