package trident.test;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class App {

	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.setNumWorkers(3);
		Map<String, String> map = new HashMap<String, String>();
		map.put("storm.zookeeper.servers", "s101");
		config.setEnvironment(map);
		
		//创建top
		TridentTopology top = new TridentTopology();
		//创建Spout
		FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("a","b","c","d"));
		
		//创建流
		Stream s = top.newStream("spout", testSpout);
		s = s.shuffle().each(new Fields("a","b"), new Filter1()).parallelismHint(2);
		s = s.broadcast().each(new Fields("a","b"), new Filter2());
		
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormApp", config,top.build());
		
		testSpout.feed(ImmutableList.of(new Values(1,1,3,4)));
		testSpout.feed(ImmutableList.of(new Values(2,1,4,5)));
		testSpout.feed(ImmutableList.of(new Values(3,5,5,6)));
		testSpout.feed(ImmutableList.of(new Values(4,5,6,7)));
		testSpout.feed(ImmutableList.of(new Values(5,8,7,8)));
		
//		Thread.sleep(10000);
//		cluster.shutdown();
		
	}

}
