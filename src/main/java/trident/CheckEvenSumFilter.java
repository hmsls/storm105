package trident;


import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * 检查是否是偶数的过滤器
 * 判断两数之和是否否偶数
 */
public class CheckEvenSumFilter extends BaseFilter {
	
	private static final long serialVersionUID = 7L;

	public boolean isKeep(TridentTuple tuple) {
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
//		int c = tuple.getInteger(2);
//		int d = tuple.getInteger(3);
		System.out.println("CheckEvenSumFilter :" + a + "," + b);
		int sum = a + b;
		if (sum % 2 == 0) {
			return true;
		}
		return false;
	}
}