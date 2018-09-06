package trident.test;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Max;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


/**
 * 固定批次Spout
 */
public class FixBatchApp {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a","b"), 3, 
					new Values(1,2),
					new Values(2,2),
					new Values(3,2),
					new Values(4,2));
		spout.setCycle(true);
		
		TridentTopology top = new TridentTopology();
		top.newStream("tx-1", spout)
			.shuffle().each(new Fields("a","b"), new Filter1()).parallelismHint(1)
			.global().each(new Fields("a","b"), new Filter2()).parallelismHint(1)
			.partitionBy(new Fields("a")).each(new Fields("a","b"), new MyFunction(), new Fields("none")).parallelismHint(2)
//			.partitionAggregate(new Fields("a"), new Count(), new Fields("count"))	//分区聚合
//			.aggregate(new Fields("a"), new SumCombinerAggregator(), new Fields("avg"))	//分区聚合
//			.persistentAggregate(new MemoryMapState.Factory(), new Fields("a"),new Count(),new Fields("count")).newValuesStream()	//持久化聚合
		
			//链式聚合
//			.chainedAgg().partitionAggregate(new Fields("a"), new Count(), new Fields("count"))
//			.partitionAggregate(new Fields("a"), new Sum(), new Fields("sum"))
//			.chainEnd()			
			
			.groupBy(new Fields("b")).partitionAggregate(new Fields("a"), new Max("a"), new Fields("a"))
			.each(new Fields("a","b"), new PrinterFunction(),new Fields("xxx"));
		Config config = new Config();
		config.setNumWorkers(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormApp", config,top.build());
//		Thread.sleep(10000);
//		cluster.shutdown();
//		StormSubmitter.submitTopology("app", config, top.build());
	}

}
