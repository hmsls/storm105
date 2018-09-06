package trident.aggregate;


import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


import clojure.lang.Numbers;
import util.MyUtil;

/**
 * 合成聚合函数
 */
public class SumCombinerAggregator implements CombinerAggregator<Number> {

	private static final long serialVersionUID = 8364282774779456048L;

	public Number init(TridentTuple tuple) {
		MyUtil.outLog2NC(this, "init("+tuple.getInteger(0)+")");
		return (Number) tuple.getValue(0);
	}

	public Number combine(Number val1, Number val2) {
		MyUtil.outLog2NC(this, "combine("+val1 + "," + val2);
		return Numbers.add(val1, val2);
	}

	public Number zero() {
		MyUtil.outLog2NC(this, "zero()");
		return 0;
	}
}
