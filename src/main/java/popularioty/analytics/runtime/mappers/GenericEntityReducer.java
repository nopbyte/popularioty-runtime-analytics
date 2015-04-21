package popularioty.analytics.runtime.mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popularioty.analytics.runtime.writable.RuntimeEdge;
import popularioty.analytics.runtime.writable.RuntimeKey;
import popularioty.analytics.runtime.writable.RuntimeVote;

public class GenericEntityReducer extends
Reducer<RuntimeKey, RuntimeVote, RuntimeKey, RuntimeEdge>  {
	
	protected void reduce(RuntimeKey key, Iterable<RuntimeVote> values, Context context) throws java.io.IOException, InterruptedException 
	{
	    RuntimeEdge edge = new RuntimeEdge();
		for(RuntimeVote v: values)
			edge.merge(v);
		context.write(key,edge);
	}
	
}
