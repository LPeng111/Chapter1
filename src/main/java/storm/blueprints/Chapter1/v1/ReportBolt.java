package storm.blueprints.Chapter1.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {
	private HashMap<String,Long> counts = null;

	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String word = arg0.getStringByField("word");
		Long count = arg0.getLongByField("count");
		this.counts.put(word, count);
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.counts = new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}
	
	public void cleanup() {
		System.out.println("--- Final Counts ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for(String key : keys) {
			System.out.println(key + " : " + this.counts.get(key));
		}
		System.out.println("--------------------");
	}
}
