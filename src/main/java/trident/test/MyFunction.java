package trident.test;


import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import util.MyUtil;


public class MyFunction extends BaseFunction {
	
	public MyFunction() {
		MyUtil.outLog2NC(this, "new MyFunction()");
	}
	
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		MyUtil.outLog2NC(this, "prepare");
		
	}

	/**
	 * a,b,none (a,b,a)
	 */
	public void execute(TridentTuple tuple, TridentCollector collector) {
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
		collector.emit(new Values(tuple.get(0)));
		MyUtil.outLog2NC(this, "excute :a=" + a + " b=" + b);
	}

}
