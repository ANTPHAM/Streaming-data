package bitcoins;

import java.io.File;
import java.util.Date;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SaveResultsBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if ("transaction-amount".equals(input.getSourceStreamId())) {
	        // this came from stream1
		 
	     } 
		
		else if ("bitcoin-block-parsing".equals(input.getSourceStreamId())) {
        // this came from stream2
         }
		try {
			process(input);
			outputCollector.ack(input);
		} catch (IOException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
		
	public void process(Tuple input) throws IOException {
		 			
		// data from stream3 "block_timestamp", "block_hash", "block_found_by"	
		//"transaction_timestamp", "total_amount_bitcoin","total_euro_amount_bitcoin","price_timestamp","price"
		Long block_timestamp = (long)0;
		String block_hash = "";
		String block_found_by = "";
		String price_timestamp = "";
		double price = (double)0;
		String transaction_timestamp = "";
		float totalAmountBitcoin = (float)0;
		float totalEuroAmountBitcoin = (float)0;
	    float max_amount_bitcoin = (float)0;
	    float max_euros_amount_bitcoin = (float)0;
		
		if ("bitcoin-block-parsing".equals(input.getSourceComponent())) {
			 block_timestamp = (Long)input.getLongByField("block_timestamp");
		     block_hash = (String)input.getStringByField("block_hash");
			 block_found_by = (String)input.getStringByField("block_found_by");
			 
			 //outputCollector.emit(new Values(block_timestamp, block_hash, block_found_by));
		}
		
		else if ("transaction-amount".equals(input.getSourceComponent())) {
			//price_timestamp = input.getStringByField("price_timestamp");
			// price = input.getDoubleByField("price");
			 transaction_timestamp = input.getStringByField("transaction_timestamp");
			 totalAmountBitcoin = input.getFloatByField("total_amount_bitcoin");
			 totalEuroAmountBitcoin = input.getFloatByField("total_euro_amount_bitcoin");
			 max_amount_bitcoin = input.getFloatByField("max_amount_bitcoin");
			 max_euros_amount_bitcoin = input.getFloatByField("max_euros_amount_bitcoin");
			 
			 //outputCollector.emit(new Values(timestamp, index, date,totalAmountBitcoin,totalEuroAmountBitcoin));
		}
						
		outputCollector.emit(new Values(transaction_timestamp, totalAmountBitcoin,totalEuroAmountBitcoin,price_timestamp,price,block_timestamp,block_hash, block_found_by,max_amount_bitcoin,max_euros_amount_bitcoin));
		
		
        //String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		String filePath = String.format("/tmp/%s.csv", transaction_timestamp);
		
		// Check if file exists
		File csvFile = new File(filePath);
		if(!csvFile.exists()) {
			FileWriter fileWriter = new FileWriter(filePath);
			fileWriter.write("transaction_timestamp;total_amount_bitcoin;total_euro_amount_bitcoin;price_timestamp;price;block_timestamp;block_hash;block_found_by;max_amount_bitcoin;max_euros_amount_bitcoin\n");
			fileWriter.close();
		}
		
		// Write stats to file
		FileWriter fileWriter = new FileWriter(filePath, true);
		
		if ("bitcoin-block-parsing".equals(input.getSourceComponent())) {
			System.out.printf("====== SaveResultsBlockBolt: %s %s %s total blocks\n",block_timestamp,block_hash,block_found_by);
		}
		
		
		else if ("transaction-amount".equals(input.getSourceComponent())) {
			System.out.printf("====== SaveResultsBolt: %s %f %f  %f %f total bitcoin\n", transaction_timestamp, totalAmountBitcoin,totalEuroAmountBitcoin,max_amount_bitcoin,max_euros_amount_bitcoin);
		}
		
		fileWriter.write(String.format("%s;%f;%f;%s;%s;%s;%f;%f\n", transaction_timestamp,totalAmountBitcoin,totalEuroAmountBitcoin,block_timestamp,block_hash, block_found_by,max_amount_bitcoin,max_euros_amount_bitcoin));
		fileWriter.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_timestamp","totalAmountBitcoin","totalEuroAmountBitcoin","block_timestamp","block_hash","block_found_by","max_amount_bitcoin"));
	}
}