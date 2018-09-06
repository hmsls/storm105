package storm105.storm105;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CallLogCreatorBolt implements IRichBolt {
	
	//Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;
	
	//Provides the bolt with an environment to execute. The executors will run this method to initialize the spout.
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	//Process a single tuple of input.
	public void execute(Tuple tuple) {
		String from = tuple.getString(0);
		String to = tuple.getString(1);
		Integer duration = tuple.getInteger(2);
		collector.emit(new Values(from + " - " + to + " - " , duration));
	}
	//Called when a bolt is going to shutdown.
	public void cleanup() {
	}
	//Declares the output schema of the tuple.
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call","duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
