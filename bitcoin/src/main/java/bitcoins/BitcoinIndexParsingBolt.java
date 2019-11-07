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

public class BitcoinIndexParsingBolt extends BaseRichBolt {
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

		String price_timestamp = (String)((JSONObject)data.get("time")).get("updated");
		JSONObject indexjson = ((JSONObject)data.get("bpi"));
		double price = (Double)((JSONObject)indexjson.get("EUR")).get("rate_float");
		
		// emit = send		
		outputCollector.emit(new Values(price_timestamp,price));
		outputCollector.ack(input);
		
		System.out.printf("====== SaveRates: %s %f bitcoin rates\n",price_timestamp,price);
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("price_timestamp","price"));
	}

}
