
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import util.MyUtil;


/**
 * Bolt
 */
public class CallLogCreatorBolt implements IRichBolt {
	//
	private OutputCollector collector;
	
	private int taskId ;
	
	public CallLogCreatorBolt(){
//		MyUtil.outLog2NC(this, "new CallLogCreatorBolt()");
		System.out.println("new CallLogCreatorBolt()");
	}

	//initialize
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		//MyUtil.outLog2NC(this, "prepare()");
		this.collector = collector;
		this.taskId = context.getThisTaskId();
	}
	

	public void execute(Tuple tuple) {
		int index = tuple.getInteger(0);
		//主叫
		String from = tuple.getString(1);
		//被叫
		String to = tuple.getString(2);
		//通话时长
		Integer duration = tuple.getInteger(3);
		
//		MyUtil.outLog2NC(this, "execute() : taskId:" + taskId + "  index:" + index + " from:" + from );
		System.out.println("execute() : taskId:" + taskId + "  index:" + index + " from:" + from );
		//
		collector.emit(new Values(index , from + " - " + to, duration));
	}

	public void cleanup() {
//		MyUtil.outLog2NC(this, "cleanup()");
		System.out.println("cleanup()");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("index","call", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;

	}
}