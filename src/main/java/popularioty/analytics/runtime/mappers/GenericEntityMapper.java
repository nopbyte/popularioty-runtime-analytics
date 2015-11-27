package popularioty.analytics.runtime.mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popularioty.analytics.runtime.start.HadoopJobs;
import popularioty.analytics.runtime.writable.RuntimeKey;
import popularioty.analytics.runtime.writable.RuntimeVote;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GenericEntityMapper  extends
Mapper<LongWritable, Text, RuntimeKey, RuntimeVote>{


	 
	protected void map(LongWritable key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {
			
		
		 Configuration conf = context.getConfiguration();
		 long since = conf.getLong("documents-since",HadoopJobs.DEFAULT_SINCE);
		 long until = conf.getLong("documents-until",HadoopJobs.DEFAULT_UNTIL);
		 
		 ObjectMapper mapper = new ObjectMapper();
		 String s1 = value.toString();
		 String s = s1.substring(s1.indexOf(",")+1,s1.length());
		 try{
				 JsonNode runtime = mapper.readTree(s);
				 long date = runtime.get("date").asLong();
				 if(since<=date && date<until)
				 {
				 
					 JsonNode contract = runtime.get("contract_compliance");//used by activity in services
					 if(contract != null){
						 JsonNode src = runtime.get("src");
						 RuntimeKey k = buildRuntimeKey(src,src);
						 emmitVoteForServiceActivity(k, contract.asBoolean(),runtime, context);
					 }
					 else{
						 //System.out.println("Document processed == date found: "+date + " since: "+since+ " until: "+until);
						 JsonNode src = runtime.get("src");
						 JsonNode dest = runtime.get("dest");
						 RuntimeKey k = buildRuntimeKey(src,dest); 
						 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_WEB_OBJECT))
							 context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_WEB_OBJECT).setValue(1));
						 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_SO_STREAM) && k.getEntityTypeDestination().equals(RuntimeKey.ENTITY_TYPE_SO_STREAM))
							 emmitVotesSOtoSO(k,runtime, context);
						 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_SO_STREAM) && k.getEntityTypeDestination().equals(RuntimeKey.ENTITY_TYPE_USER))
							 emmitVotesSOtoUser(k,runtime, context);
						 if(k.getEntityTypeSource().equals(RuntimeKey.ENTITY_TYPE_SERVICE))
							 emmitVotesPopularityServicetoService(k,runtime, context);
					 }
					 
				 }
				 /*else{
					 System.out.println("dropped: date: "+date+" since: "+since+" last: "+until);
				 }*/
				
		 }catch(Exception e){
			 //There may be ids follwed by an empty string... due to sqoop! :(
			 System.err.println("problems parsing this line: "+s1);
			 return;
		 }
		
		 //The mapper generates RuntimeVote according to the document (keep in mind in the future services are coming.. add an if for the SO check first)
		 //The reducer shall generate the SOEdge and populate it propperly
		
     }

    private void emmitVotesSOtoUser(RuntimeKey k, JsonNode runtime,
		Context context) throws IOException, InterruptedException {
		
    	context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_READ_SUBSCRIPTION).setValue(1));
		
	}

	private void emmitVotesPopularityServicetoService(RuntimeKey k, JsonNode runtime,
			Context context) throws IOException, InterruptedException 
	{
    	 context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SERVICE_POPULARITY).setValue(1));
	}

    private void emmitVoteForServiceActivity(RuntimeKey k, boolean compliance, JsonNode runtime, Context context) throws IOException, InterruptedException
    {
    	if(compliance)
    		context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SERVICE_ACTIVITY_OK).setValue(1));
    	else
    		context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SERVICE_ACTIVITY_WRONG).setValue(1));
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
		else if(discard.equals("error")) //javascript error! :)
			context.write(k, new RuntimeVote().setTypeOfVote(RuntimeVote.SU_DISCARD_JS).setValue(1));
		else if(!discard.equals("timestamp")) //timestamp stopped being relevant in the end.. 
			System.err.println("Unknown discard type in servioticy documents: "+discard);
		return;
	}
	private RuntimeKey buildRuntimeKey(JsonNode src,JsonNode dest) throws Exception 
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
		 else if(src.has("service_instance") ){
			 ret.setEntityTypeSource(RuntimeKey.ENTITY_TYPE_SERVICE);
			 ret.setEntityIdSource(src.get("service_instance").asText());			 
		 }
		 else if(src.has("unknown") ){
			 ret.setEntityTypeSource(RuntimeKey.ENTITY_TYPE_UNKNOWN);
			 ret.setEntityIdSource(RuntimeKey.ENTITY_TYPE_UNKNOWN);			 
		 }
		 else{
			 throw new Exception("unknwon source key ");//others...
		 }
		 
		 if(dest.has("soid")){
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_SO_STREAM);
			 ret.setEntityIdDestination(dest.get("soid").asText()+"#!"+dest.get("streamid").asText());			 
		 }
		 else if(dest.has("on_behalf_of")){
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_USER);
			 ret.setEntityIdDestination(dest.get("on_behalf_of").asText());			 
		 }
		 else if(dest.has("user_id")){
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_USER);
			 ret.setEntityIdDestination(dest.get("user_id").asText());
		 }
		 else if(dest.has("service_instance")){
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_SERVICE);
			 ret.setEntityIdDestination(dest.get("service_instance").asText());
		 }
		 else if(dest.has("unknown")){
			 ret.setEntityTypeDestination(RuntimeKey.ENTITY_TYPE_UNKNOWN);
			 ret.setEntityIdDestination(RuntimeKey.ENTITY_TYPE_UNKNOWN);
		 }
		 else{
			 throw new Exception("unknwon destination key ");//others...
		 }
		

		
		 return ret;		 
		
	}
}
