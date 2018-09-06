package trident.aggregate;


import java.io.Serializable;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import util.MyUtil;


/**
 * 平均值批次聚合函数
 */
public class AvgBatchAggregator extends BaseAggregator<AvgBatchAggregator.State> {
	private static final long serialVersionUID = 1L;

	// state class
	static class State implements Serializable {
		private static final long serialVersionUID = 7212820181211537049L;
		//元组值的总和
		float sum = 0;
		//元组个数
		int count = 0;
	}

	// 初始化状态
	public State init(Object batchId, TridentCollector collector) {
		MyUtil.outLog2NC(this, "init()");
		return new State();
	}

	// 在state变量中维护状态
	public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
		MyUtil.outLog2NC(this, "aggregate("+state.count+")");
		state.count = state.count + 1;
		state.sum = state.sum + tridentTuple.getInteger(0);
	}

	// 处理完成所有元组之后，返回一个具有单个值的Tuple
	public void complete(State state, TridentCollector tridentCollector) {
		MyUtil.outLog2NC(this, "complete("+state.count+")");
		tridentCollector.emit(new Values(state.sum / state.count));
	}
}