
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import util.MyUtil;


/**
 * Bolt
 */
public class CallLogCounterBolt implements IRichBolt {
	//
	Map<String, Integer> counterMap;
	//
	private OutputCollector collector;
	
	public CallLogCounterBolt(){
//		MyUtil.outLog2NC(this, "new CallLogCounterBolt()");
		System.out.println("new CallLogCounterBolt()");
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		//MyUtil.outLog2NC(this, "CallLogCounterBolt.prepare()");
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		int index = tuple.getInteger(0);
		//call
		String call = tuple.getString(1);
		//duration
		Integer duration = tuple.getInteger(2);
		
		if (!counterMap.containsKey(call)) {
			counterMap.put(call, 1);
		} else {
			Integer c = counterMap.get(call) + 1;
			counterMap.put(call, c);
		}
//		MyUtil.outLog2NC(this, "execute() : " + index);
		System.out.println("execute() : " + index);
		collector.ack(tuple);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}