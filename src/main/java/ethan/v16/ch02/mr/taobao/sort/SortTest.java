package ethan.v16.ch02.mr.taobao.sort;

/**
 * @since 2014-9-22
 * @author ethan
 */

public class SortTest {

	public static void main(String[] args) {
		String name ="ethan";
		
		System.out.println(name.hashCode() & Integer.MAX_VALUE);
		System.out.println(name.hashCode());
		System.out.println(name.hashCode() %5);
	}
}
