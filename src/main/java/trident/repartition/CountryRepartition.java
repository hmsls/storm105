package trident.repartition;


import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * 自定义再分区操作
 */
public class CountryRepartition implements CustomStreamGrouping, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Map<String, Integer> countries = ImmutableMap.of("India", 0, "Japan", 1, "United State", 2,
			"China", 3, "Brazil", 4);
	private int tasks = 0;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		tasks = targetTasks.size();
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String country = (String) values.get(0);
		return ImmutableList.of(countries.get(country) % tasks);
	}
}