package popularioty.analytics.runtime.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.mortbay.log.StdErrLog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import popularioty.analytics.runtime.services.global.SearchInstanceBuilder;
import popularioty.analytics.runtime.writable.RuntimeKey;
import popularioty.analytics.runtime.writable.RuntimeVote;
import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteriaType;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryType;

public class GenericEntityMapper  extends
Mapper<LongWritable, Text, RuntimeKey, RuntimeVote>{

	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);
	
	private Map<String,Long> getCountOfDocumentsByTerm(Map<String,String> mustMatchCriteria, String term)
	{
		return null;	
	}
			
	 
	protected void map(LongWritable key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {
			
		
		
		 ObjectMapper mapper = new ObjectMapper();
		 String s = value.toString();
		 s = s.substring(s.indexOf(",")+1,s.length());
		 
		 JsonNode runtime = mapper.readTree(s);
		 //TODO filter by timestamp?
		 
		 JsonNode src = runtime.get("src");
		 JsonNode dest = runtime.get("dest");
		 RuntimeKey k = buildRuntimeKey(src,dest); 
		 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_WEB_OBJECT))
			 context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_WEB_OBJECT).setValue(1));
		 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_SO_STREAM) && k.getEntityTypeDestination().equals(RuntimeKey.ENTITY_TYPE_SO_STREAM))
			 emmitVotesSOtoSO(k,runtime, context);
		//The mapper generates RuntimeVote according to the document (keep in mind in the future services are comming.. add an if for the SO check first)
		//The reducer shall generate the SOEdge and populate it propperly
		
		
		
		
		
		
		
		
		     /*SearchProvider search =  SearchInstanceBuilder.getInstance().getSearchProvider("search.properties");
			 Map<String,String> map = new HashMap<String, String>();
			 Query q = new Query(QueryType.AGGREGATIONS);
			 q.addCriteria(new SearchCriteria<String>("doc.src.soid",  "1426859511066689111ae7e5e46dca7066ec2bc59b3b5", SearchCriteriaType.MUST_MATCH));
			 Query internal = new Query(QueryType.AGGREGATIONS);
			 internal.addCriteria(new AggregationCriteria<String>("couchbaseDocument.doc.dest.streamid",null,AggregationCriteriaType.TERMS));
			 q.addSubQuery(internal);
			 QueryResponse resp;
			try {
			
				resp = search.execute(q, "");
				System.out.println(resp.getMapResult());
				 
			} catch (PopulariotyException e) {
				
				e.printStackTrace();
			}
			 
		     //655209;1;796764372490213;804422938115889;6 is the Sample record format
		     String[] line = value.toString().split(";");
		     // If record is of SMS CDR
		     if (Integer.parseInt(line[1]) == 1) {
		       status.set(line[4]);
		       context.write(status, addOne);
		     }*/
		
		  }


	private void emmitVotesSOtoSO(RuntimeKey k, JsonNode runtime, Context context) throws IOException, InterruptedException 
	{
		String discard = runtime.get("discard").asText();
		if(!discard.equals("none"))
			emmitDiscardVote(k, context, discard);
		else
		{
			if(runtime.get("event").asBoolean())
				context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_EVENT).setValue(1));
			else
			context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_NON_EVENT).setValue(1));
		}
	
	}


	private void emmitDiscardVote(RuntimeKey k, Context context, String discard)
			throws IOException, InterruptedException {
		if(discard.equals("filter"))
			context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_DISCARD_FILTER).setValue(1));
		else if(discard.equals("policy")) //TODO check this value
			context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_DISCARD_POLICY).setValue(1));
		else if(discard.equals("js")) //TODO check this value
			context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_DISCARD_JS).setValue(1));
		else
			System.err.println("Unknonw discard type: "+discard);
		return;
	}
	private RuntimeKey buildRuntimeKey(JsonNode src,JsonNode dest) 
	{
		 RuntimeKey ret = new RuntimeKey();
		 if(src.has("soid"))
		 {
			 ret.setEntityTypeSource(RuntimeKey.ENTITY_TYPE_SO_STREAM);
			 ret.setEntityIdSource(src.get("soid").asText()+"#!"+src.get("streamid").asText());			 
		 }
		 else if(src.has("webobject"))
		 {
			 ret.setEntityTypeSource(RuntimeKey.ENTITY_TYPE_WEB_OBJECT);
			 ret.setEntityIdSource("n");	
		 }
		 else{
			 //service or others...
		 }
		 if(dest.has("soid"))
		 {
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_SO_STREAM);
			 ret.setEntityIdDestination(dest.get("soid").asText()+"#!"+dest.get("streamid").asText());			 
		 }
		 else{
			 //service or others...
		 }
		 return ret;		 
		
	}
}
