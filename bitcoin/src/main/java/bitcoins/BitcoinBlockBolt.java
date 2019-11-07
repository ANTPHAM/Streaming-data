package bitcoins;

import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BitcoinBlockBolt extends BaseRichBolt {
private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			process(input);
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	public void process(Tuple input) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject data = (JSONObject)jsonParser.parse(input.getStringByField("value"));
		String op = (String)data.get("op");
		
		if ("block".equals(op)) {
			Long block_timestamp = (Long)((JSONObject)data.get("x")).get("time");
			String block_hash = (String)((JSONObject)data.get("x")).get("hash");
			JSONObject block = ((JSONObject)data.get("x"));
            String block_found_by = (String)((JSONObject)block.get("foundBy")).get("description");
		    
            outputCollector.emit(new Values(block_timestamp, block_hash, block_found_by));
		}
		
			
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("block_timestamp", "block_hash", "block_found_by"));
	}

}