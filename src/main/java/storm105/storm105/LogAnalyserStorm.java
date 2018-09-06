package storm105.storm105;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class LogAnalyserStorm {
	public static void main(String[] args) throws Exception {
		//Create Config instance for cluster configuration
		Config config = new Config();
		Map<String,String> map = new HashMap<String,String>();
		map.put("storm.zookeeper.servers", "60.24.64.83:2181");
		map.put("nimbus.host", "60.24.64.81");
		map.put("supervisor.slots.ports", "6700");
		config.setEnvironment(map);
		config.setDebug(false);
		//
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());
		builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt()).fieldsGrouping("call-log-creator-bolt", new Fields("call"));
		builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt()).shuffleGrouping("call-log-reader-spout");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
		Thread.sleep(10000);
		//Stop the topology
		cluster.shutdown();
	}
}
