package reliable;


import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import util.MyUtil;


/**
 * Bolt
 */
public class MyBolt implements IRichBolt {
	private static final long serialVersionUID = 1885494303644266466L;

	private OutputCollector collector;
	
	public MyBolt(){
	}

	//initialize
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String msg = tuple.getString(0);
		try {
			Thread.sleep(2000);
		} catch (Exception e) {
		}
		MyUtil.outLog2NC(this, msg + ":ack");
		collector.ack(tuple);
//		if(new Random().nextBoolean()){
//		}
//		else{
//			collector.fail(tuple);
//			MyUtil.outLog2NC(this, msg + ":fail");
//		}
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;

	}
}