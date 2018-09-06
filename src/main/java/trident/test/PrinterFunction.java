package trident.test;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import util.MyUtil;


public class PrinterFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		Fields fields= tuple.getFields();
		StringBuilder builder = new StringBuilder();
		for(String f : fields){
			builder.append(f + ":" + tuple.getValueByField(f) + " , " );
		}
		MyUtil.outLog2NC(this, builder.toString());
	}

}
