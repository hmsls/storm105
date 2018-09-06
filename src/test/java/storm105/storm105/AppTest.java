package storm105.storm105;


import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import util.MyUtil;


/**
 * Unit test for simple App.
 */
public class AppTest {

	@Test
	public void test() throws Exception {
		MyUtil.outLog2NC(this, "hello");
	}
	
	@Test
	public void testArray(){
		int i = 100 ;
		System.out.println(MyUtil.bytes2int(MyUtil.int2Bytes(i)));
	}
	
	@Test
	public void iniZKIndex() throws Exception{
		ZooKeeper zk = new ZooKeeper("s101:2181", 500, new Watcher(){
			public void process(WatchedEvent arg0) {
			}});
		zk.create("/index", MyUtil.int2Bytes(0),  org.apache.storm.shade.org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.storm.shade.org.apache.zookeeper.CreateMode.PERSISTENT);
	}
	
	/**
	 * 输出日志到nc服务器中
	 * @throws Exception 
	 */
	@Test
	public void outLog2NCServer() throws Exception{
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec("nc localhost 8888");
		OutputStream os = p.getOutputStream();
		for(int i = 0 ; i < 100 ; i ++){
			os.write((getLog() + "tom" + i + "\n").getBytes());
		}
		os.close();
	}
	
	public String getLog() throws Exception{
		//date
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
		String time = sdf.format(date);
		
		//host
		String host = InetAddress.getLocalHost().getHostName();
		
		//PID
		RuntimeMXBean rr = ManagementFactory.getRuntimeMXBean();
		String pid = rr.getName().split("@")[0];			//pid@s100
		
		//tname tid
		String tname = Thread.currentThread().getName();
		long tid = Thread.currentThread().getId() ;
		String tinfo = tname+"-" + tid ;
		
		//class@hash
		String className = this.getClass().getSimpleName();
		int hash = this.hashCode() ;
		String hashInfo = className + "@" + hash ;
		
		String log = "["+time + " " + host + " PID:" + pid + " " +tinfo + " " + hashInfo +"] " ;
		return log ;
	}
	
}
