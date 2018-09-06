package reliable;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import util.MyUtil;


/**
 * Spout
 */
public class MySpout implements IRichSpout {
	//收集器
	private SpoutOutputCollector collector;
	
	//环境信息
	private TopologyContext context;
	
	//发送的消息
	private  Map<Integer, String> toSend = new HashMap<Integer, String>();
	private Map<Integer, String> allMessage = new HashMap<Integer, String>();
	private Map<Integer, Integer> failMessage = new HashMap<Integer, Integer>();
	
	
	//最大重试次数
	private static int MAX_RETRY = 3 ;
	
	//initialize
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		toSend = Collections.synchronizedMap(toSend);
		failMessage = Collections.synchronizedMap(failMessage);
		this.context = context;
		this.collector = collector;
		//初始化消息集合
		for(int i = 0 ; i < 10 ; i ++){
			toSend.put(i, "" + i + ",tom" + i + "," + (10 + i));
			allMessage.put(i, "" + i + ",tom" + i + "," + (10 + i));
		}
	}

	public void nextTuple() {
		if (toSend != null && !toSend.isEmpty()) {
			for(Entry<Integer, String> entry : toSend.entrySet()){
				//发送消息，附带messageId
				collector.emit(new Values(entry.getValue()),entry.getKey());
			}
			//清除发送的消息集合
			toSend.clear();
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

	/**
	 * release
	 */
	public void close() {
	}

	public boolean isDistributed() {
		return false;
	}

	public void activate() {
	}

	public void deactivate() {
	}

	/**
	 * 成功处理消息的回调方法
	 */
	public void ack(Object msgId) {
		MyUtil.outLog2NC(this, "" + msgId);
		int index = (Integer)msgId ;
		toSend.remove(index);
		failMessage.remove(index);
	}

	/**
	 * 消息处理失败的方法
	 */
	public void fail(Object msgId) {
		//取出消息编号
		Integer index = (Integer)msgId ;
		//判断该消息失败的次数
		Integer count = failMessage.get(index);
		count = count == null ? 0 : count ;
		if(count < MAX_RETRY){
			failMessage.put(index, count + 1);
			toSend.put(index, allMessage.get(index));
		}
		else{
			MyUtil.outLog2NC(this, "msg:" + index + " has been reached max retries!");
			toSend.remove(index);
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}