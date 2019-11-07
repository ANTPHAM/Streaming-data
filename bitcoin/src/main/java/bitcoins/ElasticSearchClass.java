package bitcoins;

import org.elasticsearch.client.transport.TransportClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;


public class ElasticSearchClass {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    //private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    //private static final String INDEX = "bitcoins";
    public static final String TRANSACTION_TYPE = "transactions";
    public static final String TRANSACTION_RAW_TYPE = "transactions_raw";
    public static final String BLOCK_TYPE= "blocks";
    public static final String RATES_TYPE= "change_rates";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     * @return RestHighLevelClient+28
     */
    private static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME)));
                            //new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    public static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

   
    public void sendData(String index,String id,Map data) {
    	makeConnection();
   
    	//id=UUID.randomUUID().toString();
        IndexRequest indexRequest = new IndexRequest(index)
        		.id(id)
                .source(data);
        try {
        	//RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder(); => use this to customize options
        
            IndexResponse response = restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
            throw new RuntimeException(e);
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
            throw new RuntimeException(ex);
        }
    }
}

  	
