package trident.drpc.local;


import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ExclaimBolt implements IRichBolt {

	private static final long serialVersionUID = 4365167421259301356L;
	OutputCollector collector ;
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector ;
	}

	public void execute(Tuple tuple) {
		 String input = tuple.getString(1);
		 String[] arr = input.split(",");
		 int a = Integer.parseInt(arr[0]);
		 int b = Integer.parseInt(arr[1]);
		 
	     collector.emit(new Values(tuple.getValue(0), a + b));
	}

	public void cleanup() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
