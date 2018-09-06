package kafka;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;

/**
 * 生成句子的bolt
 */
public class SentenceBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1113117905744601061L;
	private List<String> words = new ArrayList<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		if (StringUtils.isBlank(word)) {
			return;
		}
		System.out.println("Received Word:" + word);
		words.add(word);
		if (word.endsWith(".")) {
			collector.emit(ImmutableList.of((Object) StringUtils.join(words, ' ')));
			words.clear();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
}