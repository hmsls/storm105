package trident.aggregate;


import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * 自定义sum聚合函数
 */
public class Sum implements ReducerAggregator<Long> {
	private static final long serialVersionUID = 1L;

	public Long init() {
		return 0L;
	}

	//curr作为反复使用的值变量
	public Long reduce(Long curr, TridentTuple tuple) {
		return curr + tuple.getInteger(0) + tuple.getInteger(1);
	}
}