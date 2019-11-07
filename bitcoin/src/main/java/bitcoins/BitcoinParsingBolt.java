package bitcoins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.command.list;
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

public class BitcoinParsingBolt extends BaseRichBolt {
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
		
		if ("utx".equals(op)) {
			Long transaction_timestamp = (Long)((JSONObject)data.get("x")).get("time");
			String transaction_hash = (String)((JSONObject)data.get("x")).get("hash");
			float transaction_total_amount = 0;
			
			JSONArray outs = (JSONArray)((JSONObject)data.get("x")).get("out");
			for (Object outObj : outs) {
				//value is Long but to prevent rounding after division by 100000000 need to cast first to float
				transaction_total_amount +=((float)(Long)((JSONObject)outObj).get("value"))/ 100000000;
			}
						
			
	        // emit = send		
			outputCollector.emit(new Values(transaction_timestamp, transaction_hash, transaction_total_amount));
		}
					
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_timestamp", "transaction_hash", "transaction_total_amount"));
	}

}
