
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
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
public class FakeCallLogReaderSpout implements IRichSpout {
	//收集器
	private SpoutOutputCollector collector;
	
	//环境信息
	private TopologyContext context;
	
	//
	private boolean completed = false;
	
	List<Integer> ids ;
	
	
	//
	private Random randomGenerator = new Random();
	
	private int index = 0 ;
	
	public FakeCallLogReaderSpout(){
		try {
//			MyUtil.outLog2NC(this, "new FakeCallLogReaderSpout()");
			System.out.println("new FakeCallLogReaderSpout()");
		} catch (Exception e) {
		}
	}
	
	//initialize
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		//MyUtil.outLog2NC(this, "open()");
		this.context = context;
		this.collector = collector;
		
		ids = context.getComponentTasks("bolt-1");
//		for(Integer id : ids){
//			MyUtil.outLog2NC(this, "taskId = " + id);
//		}
	}

	/**
	 * 
	 */
	public void nextTuple() {
		if (index <= 10) {
			//电话集合
			List<String> mobileNumbers = new ArrayList<String>();
			mobileNumbers.add("1234123401");
			mobileNumbers.add("1234123402");
			mobileNumbers.add("1234123403");
			mobileNumbers.add("1234123404");
			for (; index < 10 ;index ++) {
				//主叫
				String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				//被叫
				String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				
				while (fromMobileNumber == toMobileNumber) {
					toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				}
				//生成随机数,表示通话时长
				Integer duration = randomGenerator.nextInt(60);
				
//				MyUtil.outLog2NC(this,"nextTuple() : " + index + ":" + fromMobileNumber);
				System.out.println("nextTuple() : " + index + ":" + fromMobileNumber);
				//输出通话记录
				this.collector.emit(new Values(index,fromMobileNumber, toMobileNumber, duration));
			}
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("index","from", "to", "duration"));
	}

	/**
	 * release
	 */
	public void close() {
		//MyUtil.outLog2NC(this, "close()");
	}

	public boolean isDistributed() {
		return false;
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void ack(Object msgId) {
		//MyUtil.outLog2NC(this, "ack("+msgId+")");
	}

	public void fail(Object msgId) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	public int getIndexFromZK(){
		try {
			ZooKeeper zk = new ZooKeeper("s101:2181", 500, new Watcher(){
				public void process(WatchedEvent arg0) {
				}});
			
			Stat s = new Stat() ;
			byte[] b = zk.getData("/index", null, s );
			int i = MyUtil.bytes2int(b);
			zk.setData("/index", MyUtil.int2Bytes(i + 1), s.getVersion());
			return i ;
		} catch (Exception e) {
		}
		return -1 ;
	}
}