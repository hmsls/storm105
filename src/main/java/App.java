
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		
		Config config = new Config();
		//config.setDebug(true);
		config.setNumWorkers(3);
		Map<String, String> map = new HashMap<String, String>();
		map.put("storm.zookeeper.servers", "s101");
		config.setEnvironment(map);
		//builder
		TopologyBuilder builder = new TopologyBuilder();
		
		//设置spout
		//2:执行线程数 ,4:spout任务数
		builder.setSpout("spout", new FakeCallLogReaderSpout(),2)
			.setNumTasks(4);
		
		//设置bolt
		builder.setBolt("bolt-1", new CallLogCreatorBolt(),3)
			.shuffleGrouping("spout")
			.setNumTasks(6);
		//设置bolt,设置分组策略
		builder.setBolt("bolt-2", new CallLogCounterBolt(),4)
			.fieldsGrouping("bolt-1",new Fields("call"))
			.setNumTasks(8);
		
		//本地集群模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormApp", config, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
//		StormSubmitter.submitTopology("StormApp", config, builder.createTopology());
	}
}
