package reliable;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		
		Config config = new Config();
		config.setDebug(true);
		Map<String, String> map = new HashMap<String, String>();
		map.put("storm.zookeeper.servers", "s101");
		config.setEnvironment(map);
		config.setMessageTimeoutSecs(1);
		//builder
		TopologyBuilder builder = new TopologyBuilder();
		
		//设置spout,
		builder.setSpout("myspout", new MySpout(),1);
		
		//设置bolt,设置分组策略
		builder.setBolt("mybolt", new MyBolt(),3).shuffleGrouping("myspout");
		
		//本地集群模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("myapp", config, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
		//StormSubmitter.submitTopology("StormApp", config, builder.createTopology());
	}
}
