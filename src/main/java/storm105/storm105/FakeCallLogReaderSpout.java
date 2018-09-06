package storm105.storm105;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeCallLogReaderSpout implements IRichSpout {
	
	//Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;
	private boolean completed = false;
	
	//Create instance for TopologyContext which contains topology data.
	private TopologyContext context;
	
	//Create instance for Random class.
	private Random randomGenertor = new Random();
	private Integer idx = 0;
	
	//Provides the spout with an environment to execute. The executors will run this method to initialize the spout.
	public void open(Map arg0, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}
	//	Emits the generated data through the collector
	public void nextTuple() {
		if(idx <= 1000){
			List<String> mobileNumbers = new ArrayList<String>();
			mobileNumbers.add("1234123401");
			mobileNumbers.add("1234123402");
			mobileNumbers.add("1234123403");
			mobileNumbers.add("1234123404");
			
			Integer localIdx = 0;
			while(localIdx++<100 && idx++  < 1000){
				String fromMobileNumber = mobileNumbers.get(randomGenertor.nextInt(4));
				String toMobileNumber = mobileNumbers.get(randomGenertor.nextInt(4));
				while(fromMobileNumber == toMobileNumber){
					toMobileNumber = mobileNumbers.get(randomGenertor.nextInt(4));
				}
				Integer duration = randomGenertor.nextInt(60);
				
				this.collector.emit(new Values(fromMobileNumber,toMobileNumber,duration));
			}
		}
	}
	//Declares the output schema of the tuple.
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("from","to","duration"));
	}
	//Acknowledges that a specific tuple is processed
	public void ack(Object arg0) {
	}

	public void activate() {
	}
	//
	public void close() {
	}

	public void deactivate() {
	}
	//Specifies that a specific tuple is not processed and not to be reprocessed
	public void fail(Object arg0) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
