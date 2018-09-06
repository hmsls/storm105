
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

/**
 * 自定义分组策略
 */
public class MyGrouping implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = -770774164055779138L;
	
	private int maxId = Integer.MIN_VALUE ;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		for(Integer id : targetTasks){
			maxId = id < maxId ? maxId : id ;
			System.out.println("taskId: " + id);
		}
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> ids = new ArrayList<Integer>();
		ids.add(maxId);
		return ids;
	}
}
