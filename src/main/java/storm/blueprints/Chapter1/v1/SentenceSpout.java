package storm.blueprints.Chapter1.v1;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	
	private String[] sentences = {
			"my dog has fleas",
			"I like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"I don't think I like fleas"
		};
	private int index = 0;

	public void nextTuple() {
		// TODO Auto-generated method stub
		this.collector.emit(new Values(sentences[index]));
		index ++;
		if(index >= sentences.length)
			index = 0;
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("sentences"));
	}

}
