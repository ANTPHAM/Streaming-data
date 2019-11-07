package bitcoins;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

//
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    	TopologyBuilder builder = new TopologyBuilder();
    	//  create a Spout to get data about transactions 
    	// Création d'un objet KafkaSpoutConfigBuilder 
        // On passe au constructeur l'adresse d'un broker Kafka ainsi que 
        // le nom d'un topic KafkaSpoutConfig.Builder
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("localhost:9092", "bitcoin_price");
    	// On définit ici le groupe Kafka auquel va appartenir le spout
    	spoutConfigBuilder.setGroupId("bitcoin_price_dashboard"); 
    	// Création d'un objet KafkaSpoutConfig
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	// Création d'un objet KafkaSpout
    	builder.setSpout("bitcoins", new KafkaSpout<String, String>(spoutConfig));
    	
    	//  create a Spout to get data about bitcoin price  index 
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder1 = KafkaSpoutConfig.builder("localhost:9092", "bitcoin_index");
    	spoutConfigBuilder1.setGroupId("bitcoin_index_dashboard");
    	KafkaSpoutConfig<String, String> spoutConfig1 = spoutConfigBuilder1.build();
    	builder.setSpout("bitcoins1", new KafkaSpout<String, String>(spoutConfig1));
    	
    	builder.setBolt("bitcoin-parsing", new BitcoinParsingBolt())
    	.shuffleGrouping("bitcoins");
    	
    	builder.setBolt("bitcoin-block-parsing", new BitcoinBlockBolt())/*builder.setBolt("bitcoin-block-parsing", new BitcoinBlockBolt(),2)*/
    	.shuffleGrouping("bitcoins");
    	
    	builder.setBolt("bitcoin-index-parsing", new BitcoinIndexParsingBolt())
		.shuffleGrouping("bitcoins1");
    	
    	var duration = BaseWindowedBolt.Duration.seconds(60);
    	//BaseWindowedBolt.Count.of(3)
    	builder.setBolt("transaction-amount", new TransactionAmountBolt().withTumblingWindow(duration))
    	.fieldsGrouping("bitcoin-parsing", new Fields("transaction_hash"))
    	.allGrouping("bitcoin-index-parsing");//, new Fields("timestamp"));
    	
    	builder.setBolt("save-results",  new SaveResultsBolt())
    	.fieldsGrouping("transaction-amount", new Fields("transaction_timestamp"))
    	.fieldsGrouping("bitcoin-block-parsing", new Fields("block_hash"));
    	//.fieldsGrouping("bitcoin-index-parsing", new Fields("price_timestamp")); 
    	
    	builder.setBolt("elastic-search",  new ElasticSearchBolt())
    	.shuffleGrouping("transaction-amount")
    	.fieldsGrouping("bitcoin-block-parsing", new Fields("block_hash"))
    	.allGrouping("bitcoin-index-parsing")
    	.fieldsGrouping("bitcoin-parsing", new Fields("transaction_hash"));;
    	
    	

    	
    	
    	
    	StormTopology topology = builder.createTopology();

    	Config config = new Config();
    	config.setMessageTimeoutSecs(60*30);
    	String topologyName = "Bitcoin"; 
    	if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(topologyName, config, topology);
    	}
    	else {
    		LocalCluster cluster;
			try {
				cluster = new LocalCluster();
	        	cluster.submitTopology(topologyName, config, topology);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
}