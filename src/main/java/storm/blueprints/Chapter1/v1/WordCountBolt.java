package storm.blueprints.Chapter1.v1;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {
	private OutputCollector collector;
	private HashMap<String,Long> counts = null;
	
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String word = arg0.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null)
			count = 0L;
		count ++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word,count));
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		counts = new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("word","count"));
	}

}
