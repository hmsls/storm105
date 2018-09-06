package kafka;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * 打印句子
 */
public class PrinterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 7556872750308232836L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		System.out.println("Received Sentence: " + sentence);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}