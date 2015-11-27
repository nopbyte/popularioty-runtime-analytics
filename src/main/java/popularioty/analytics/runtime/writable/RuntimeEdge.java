package popularioty.analytics.runtime.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * This class represents an edge for the runtime analysis between two entities 
 * @author dp
 *
 */
public class RuntimeEdge implements Writable
{
	/**
	 * Data provided from the outside
	 */
	private long webObject;
	/**
	 * When the event produces computation
	 */
	private long event;
	/**
	 * Old data was used
	 */
	private long non_event;
	/**
	 * discarded computations due to wrong js
	 */
	private long discard_js;
	/**
	 * discarded due to wrong policy
	 */
	private long discard_policy;
	/**
	 * discarded due to postfilter
	 */
	private long discard_filter;
	
	private long service_activity_ok; 
	
	private long service_activity_wrong;
	
	private long service_popularity;
	/**
	 * When a SU is sent to a subscription
	 */
	private long so_read_subscription;
	
	public RuntimeEdge() {
		
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		webObject = in.readLong();
		event= in.readLong();
		non_event= in.readLong();
		discard_js= in.readLong();
		discard_policy= in.readLong();
		discard_filter= in.readLong();
		service_popularity = in.readLong();
		service_activity_ok = in.readLong(); 
		service_activity_wrong = in.readLong();
		so_read_subscription = in.readLong();
			}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(webObject);
		out.writeLong(event);
		out.writeLong(non_event);
		out.writeLong(discard_js);
		out.writeLong(discard_policy);
		out.writeLong(discard_filter);
		out.writeLong(service_popularity);
		out.writeLong(service_activity_ok); 
		out.writeLong(service_activity_wrong);
		out.writeLong(so_read_subscription);
		
		
	}	
	
	@Override
	public String toString() {

		return webObject+"\t"
				+event+"\t"
				+non_event+"\t"
				+discard_js+"\t"
				+discard_policy+"\t"
				+discard_filter+"\t"
				+service_popularity+"\t"
				+service_activity_ok+"\t" 
				+service_activity_wrong+"\t" 
				+so_read_subscription+"\t";
	}


	public void merge(RuntimeVote vote)
	{
		
		if(vote.getTypeOfVote().equals(RuntimeVote.SU_DISCARD_FILTER))
			discard_filter +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_DISCARD_JS))
			discard_js  +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_DISCARD_POLICY))
				 discard_policy +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_EVENT))
			 event +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_NON_EVENT))
			 non_event +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_WEB_OBJECT))
			 webObject +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SERVICE_ACTIVITY_OK))
			 service_activity_ok +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SERVICE_ACTIVITY_WRONG))
			 service_activity_wrong +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SERVICE_POPULARITY))
			 service_popularity +=vote.getValue();
		else if(vote.getTypeOfVote().equals(RuntimeVote.SU_READ_SUBSCRIPTION))
			so_read_subscription+=vote.getValue();
	}
	public void merge(RuntimeEdge e)
	{
		webObject +=e.getWebObject(); 
		event +=e.getEvent();
		non_event +=e.getNon_event();
		discard_js +=e.getDiscard_js();
		discard_policy +=e.getDiscard_policy();
		discard_filter +=e.getDiscard_filter();
		service_popularity+=e.getService_popularity();
		service_activity_ok+=e.getService_activity_ok();
		service_activity_wrong+=e.getService_activity_wrong();
		so_read_subscription+=e.getSo_read_subscription();
		
	}
	
	public long getWebObject() {
		return webObject;
	}

	public void setWebObject(long webObject) {
		this.webObject = webObject;
	}

	public long getEvent() {
		return event;
	}

	public void setEvent(long event) {
		this.event = event;
	}

	public long getNon_event() {
		return non_event;
	}

	public void setNon_event(long non_event) {
		this.non_event = non_event;
	}

	public long getDiscard_js() {
		return discard_js;
	}

	public void setDiscard_js(long discard_js) {
		this.discard_js = discard_js;
	}

	public long getDiscard_policy() {
		return discard_policy;
	}

	public void setDiscard_policy(long discard_policy) {
		this.discard_policy = discard_policy;
	}

	public long getDiscard_filter() {
		return discard_filter;
	}

	public void setDiscard_filter(long discard_filter) {
		this.discard_filter = discard_filter;
	}


	public long getService_activity_ok() {
		return service_activity_ok;
	}


	public void setService_activity_ok(long service_activity_ok) {
		this.service_activity_ok = service_activity_ok;
	}


	public long getService_activity_wrong() {
		return service_activity_wrong;
	}


	public void setService_activity_wrong(long service_activity_wrong) {
		this.service_activity_wrong = service_activity_wrong;
	}


	public long getService_popularity() {
		return service_popularity;
	}


	public void setService_popularity(long service_popularity) {
		this.service_popularity = service_popularity;
	}


	public long getSo_read_subscription() {
		return so_read_subscription;
	}


	public void setSo_read_subscription(long so_read_subscription) {
		this.so_read_subscription = so_read_subscription;
	}
	

		
}
