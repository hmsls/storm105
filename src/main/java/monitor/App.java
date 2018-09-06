package monitor;


import java.lang.reflect.Method;
import java.util.List;

import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.scheduler.Topologies;

public class App {
	public static void main(String[] args) throws Exception {
		ThriftClient tc = new ThriftClient();
		Client client = tc.getClient();
		ClusterSummary sum = client.getClusterInfo();
		List<SupervisorSummary> list = sum.get_supervisors();
		for(SupervisorSummary s : list){
			Class<SupervisorSummary> clazz = SupervisorSummary.class ;
			Method[] ms = clazz.getDeclaredMethods();
			for(Method m :ms){
				if(m.getName().startsWith("get") && (m.getParameters() == null 
						|| m.getParameters().length == 0)){
					Object ret = m.invoke(s);
					System.out.println(m.getName() + " : " + ret);
				}
			}
			System.out.println("==============================");
		}
		
		//
		List<TopologySummary> tsum = sum.get_topologies();
//		String nimbusConf = client.getNimbusConf();
//		
//		System.out.println(sum);
	}
	
}
