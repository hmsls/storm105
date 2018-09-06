package kafka;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * kafka top
 */
public class KafkaTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		//zk连接串
		ZkHosts zkHosts = new ZkHosts("s101:2181");
		
		//kafka主题信息
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "test2", "", "id7");
		
		//
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		// kafkaConfig.fforceFromStart = true;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("SentenceBolt", new SentenceBolt(), 1).globalGrouping("KafkaSpout");
		builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("SentenceBolt");
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		cluster.submitTopology("KafkaToplogy", conf, builder.createTopology());
		try {
			System.out.println("Waiting to consume from kafka");
			Thread.sleep(60000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		//cluster.killTopology("KafkaToplogy");
		//cluster.shutdown();
	}
}