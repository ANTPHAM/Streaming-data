package bitcoins;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class TransactionAmountBolt extends BaseWindowedBolt{
	private OutputCollector outputCollector;
	static double latestConversionRate = (double) 0;
		
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
    	float totalAmount = (float)0;
		String transaction_timestamp = "";
		List<Float> transaction_total_amount_list = new ArrayList<>();
		for(Tuple input : inputWindow.get()) {
			if ("bitcoin-index-parsing".equals(input.getSourceComponent())) {
				latestConversionRate  = input.getDoubleByField("price");
				//price_timestamp = input.getStringByField("price_timestamp");
				
				outputCollector.ack(input);
				continue;
			}
			totalAmount += input.getFloatByField("transaction_total_amount");
			transaction_timestamp = String.valueOf(input.getLongByField("transaction_timestamp"));
			transaction_total_amount_list.add(input.getFloatByField("transaction_total_amount"));
			outputCollector.ack(input);
		}
		
		float euroAmount = (float)(totalAmount * latestConversionRate); 
		//float max_amount_bitcoin = (float) Math.max(transaction_total_amount_list);	
		float max_amount_bitcoin = (float)0;
		float max_euros_amount_bitcoin = (float)0;
		if (transaction_total_amount_list.isEmpty()){
			max_amount_bitcoin = (float)0;
			max_euros_amount_bitcoin = (float)0;
		}
		else {
			max_amount_bitcoin = (float) Collections.max(transaction_total_amount_list);
			max_euros_amount_bitcoin = (float)(max_amount_bitcoin * latestConversionRate);
		}
		
		
		// Emit average stats, city by city
		
		outputCollector.emit(new Values( transaction_timestamp, totalAmount,euroAmount,max_amount_bitcoin, max_euros_amount_bitcoin));
			
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_timestamp","total_amount_bitcoin","total_euro_amount_bitcoin","max_amount_bitcoin","max_euros_amount_bitcoin"));
	}
}

