package trident.drpc.local;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.DRPCClient;

public class MyLocalDRPC {
	public static void main(String[] args) throws Exception {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
	    builder.addBolt(new ExclaimBolt(), 3);
		
	    //LocalDRPC drpc = new LocalDRPC();
	    //LocalCluster cluster = new LocalCluster();
//	    cluster.submitTopology("drpc-demo", new Config(), builder.createLocalTopology(drpc));
//	    System.out.println("Results for 'hello':" + drpc.execute("exclamation","1,2"));
//	    cluster.shutdown();
//	    drpc.shutdown();
	    //远程drpc
	    Config config = new Config();
		config.setNumWorkers(3);
		Map<String, String> map = new HashMap<String, String>();
		map.put("storm.zookeeper.servers", "s101");
		map.put("drpc.servers", "s100");
		config.setEnvironment(map);
		
	    StormSubmitter.submitTopology("exclamation-drpc", config, builder.createRemoteTopology());
	    DRPCClient client = new DRPCClient(config,"s100", 3772);
		System.out.println(client.execute("exclamation", "2,3"));
	}
}
