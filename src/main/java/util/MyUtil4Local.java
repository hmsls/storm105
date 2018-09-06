package util;


import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.Socket;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyUtil4Local {
	/**
	 * 输出日志 到nc server
	 */
	private static OutputStream os = null;
	static{
		try {
			Socket s = new Socket("192.168.231.1", 8888);
			os = s.getOutputStream();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void outLog2NC(Object o, String msg) {
		try {
			
			String prefix = "";

			// 取得系统时间
			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
			String time = sdf.format(date);

			// hostname
			String host = InetAddress.getLocalHost().getHostName();

			// pid
			RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
			// 8808@s100
			String pid = r.getName().split("@")[0];

			// thread
			String tname = Thread.currentThread().getName();
			long tid = Thread.currentThread().getId();
			String tinfo = "TID:" + tid;

			String oclass = o.getClass().getSimpleName();
			int hash = o.hashCode();
			DecimalFormat df = new DecimalFormat("000,000,000");
			String strHash = df.format(hash);
			String oinfo = oclass + "@" + strHash;

			prefix = "[" + time + " " + host + " " + pid + " " + tinfo + " " + oinfo + "] " + msg + "\r\n";

			os.write(prefix.getBytes());
			os.flush();
//			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static byte[] int2Bytes(int i) {
		byte[] arr = new byte[4];
		arr[0] = (byte) (i >>> 24);
		arr[1] = (byte) (i >>> 16);
		arr[2] = (byte) (i >>> 8);
		arr[3] = (byte) (i >>> 0);

		return arr;
	}

	public static int bytes2int(byte[] arr) {
		return arr[0] << 24 | arr[1] << 16 | arr[2] << 8 | arr[3];
	}
}
