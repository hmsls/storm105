package trident.test;


import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import util.MyUtil;


/**
 * 检查是否是偶数的过滤器
 * 判断两数之和是否否偶数
 */
public class Filter2 extends BaseFilter {
	
	private static final long serialVersionUID = 7L;
	
	public Filter2() {
		MyUtil.outLog2NC(this,"new Filter()");
	}
	
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		MyUtil.outLog2NC(this,"prepare()");
	}

	public boolean isKeep(TridentTuple tuple) {
		int s = tuple.getInteger(0);
		MyUtil.outLog2NC(this,"filter2 : " + s);
		return true;
	}
}