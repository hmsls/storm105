package trident;


import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.BatchSpoutExecutor;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

public class MyTridentSpout extends BatchSpoutExecutor{
	private static final long serialVersionUID = 1L;

	public MyTridentSpout(IBatchSpout spout) {
		super(spout);
	}

	
}
