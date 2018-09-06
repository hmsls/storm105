package trident.aggregate;


import java.io.Serializable;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import util.MyUtil;


/**
 * 
 */
public class SumAsAggregator extends BaseAggregator<SumAsAggregator.State> {
	private static final long serialVersionUID = 1L;

	// state class
	static class State implements Serializable {
		private static final long serialVersionUID = 7212820181211537049L;
		
		long count = 0;
	}

	// Initialize the state
	public State init(Object batchId, TridentCollector collector) {
		MyUtil.outLog2NC(this, "init()");
		return new State();
	}

	// Maintain the state of sum into count variable.
	public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
		MyUtil.outLog2NC(this, "aggregate("+state.count+")");
		state.count = tridentTuple.getInteger(0) + state.count;
	}

	// return a tuple with single value as output
	// after processing all the tuples of given batch.
	public void complete(State state, TridentCollector tridentCollector) {
		MyUtil.outLog2NC(this, "complete("+state.count+")");
		tridentCollector.emit(new Values(state.count));
	}
}