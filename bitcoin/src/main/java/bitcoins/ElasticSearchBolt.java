package bitcoins;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.lang.Object;

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
import java.util.List;
import org.elasticsearch.common.Strings;

public class ElasticSearchBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private ElasticSearchClass elasticSearchClass; 
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;

		elasticSearchClass = new ElasticSearchClass();
			 
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

		if ("bitcoin-block-parsing".equals(input.getSourceComponent())) {
			Map json = new HashMap<String,String>();	
	       	json.put("block_timestamp", input.getLongByField("block_timestamp"));
	       	json.put("block_hash", input.getStringByField("block_hash"));
	       	json.put("block_found_by", input.getStringByField("block_found_by"));
	       	
	       	this.elasticSearchClass.sendData(ElasticSearchClass.BLOCK_TYPE, input.getStringByField("block_hash"), json);
		}
		
		else if ("transaction-amount".equals(input.getSourceComponent())){
			Map json = new HashMap<String,String>();	
			
			// keep the order of variables as in the SaveResultsBolt
	        //outputCollector.emit(new Values(transaction_timestamp, totalAmountBitcoin,totalEuroAmountBitcoin,price_timestamp,price,block_timestamp,block_hash, block_found_by));
	       	json.put("transaction_timestamp", input.getStringByField("transaction_timestamp"));
	       	json.put("total_amount_bitcoin", input.getFloatByField("total_amount_bitcoin"));
	       	json.put("total_euro_amount_bitcoin", input.getFloatByField("total_euro_amount_bitcoin"));
	       	json.put("max_amount_bitcoin", input.getFloatByField("max_amount_bitcoin"));
	       	json.put("max_euros_amount_bitcoin", input.getFloatByField("max_euros_amount_bitcoin"));
	       	
	       	this.elasticSearchClass.sendData(ElasticSearchClass.TRANSACTION_TYPE, UUID.randomUUID().toString(), json);
	       	
		}
			
		else if ("bitcoin-index-parsing".equals(input.getSourceComponent())) {
			Map json = new HashMap<String,String>();
			json.put("price_timestamp", input.getStringByField("price_timestamp"));
	       	json.put("price", input.getDoubleByField("price"));
	       	
	       	this.elasticSearchClass.sendData(ElasticSearchClass.RATES_TYPE, UUID.randomUUID().toString(),json);
		       	
		     }
		
		else if ("bitcoin-parsing".equals(input.getSourceComponent())) {
			Map json = new HashMap<String,String>();
			json.put("transaction_timestamp", input.getLongByField("transaction_timestamp"));
	       	json.put("transaction_hash", input.getStringByField("transaction_hash"));
	       	json.put("transaction_total_amount", input.getFloatByField("transaction_total_amount"));
	       
	       	this.elasticSearchClass.sendData(ElasticSearchClass.TRANSACTION_RAW_TYPE, UUID.randomUUID().toString(),json);
		       	
		     }
	       	
		outputCollector.ack(input);  	
		}
		
	     
			
	public ElasticSearchBolt() {

	}


	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("source", "id", "type","index"));

	}

}
