package monitor;


import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

public class ThriftClient {

	public Client getClient() {
		//storm ui的addr
		TSocket socket = new TSocket("s100", 6627);
		TFramedTransport tFramedTransport = new TFramedTransport(socket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		Client c = new Client(tBinaryProtocol);
		try {
			tFramedTransport.open();
		} catch (Exception exception) {
			throw new RuntimeException("Error occurred while making connection with us thrift server");
		}
		return c ;
	}
}