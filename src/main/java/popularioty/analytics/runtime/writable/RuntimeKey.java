package popularioty.analytics.runtime.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class RuntimeKey implements WritableComparable<RuntimeKey>{
	
	public static final String ENTITY_TYPE_WEB_OBJECT = "webobject";
	public static String ENTITY_TYPE_SO_STREAM = "service_object_stream";
	public static String ENTITY_TYPE_SERVICE = "service_instance";
	
	private String entityIdSource;
	private String entityTypeSource;
	private String entityIdDestination;
	private String entityTypeDestination;

	public RuntimeKey()
	{
		
	}
		@Override
	public void readFields(DataInput in) throws IOException {
		entityIdSource = in.readUTF();
		entityTypeSource = in.readUTF();
		entityIdDestination = in.readUTF();
		entityTypeDestination = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(entityIdSource);
		out.writeUTF(entityTypeSource);
		out.writeUTF(entityIdDestination);
		out.writeUTF(entityTypeDestination);	
	
	}

	@Override
	public String toString() {

		return entityTypeSource+"\t"+
				entityIdSource+"\t"+
				entityTypeDestination+"\t"+
				entityIdDestination;
	}

	@Override
	public int compareTo(RuntimeKey o) {
		return ComparisonChain.start().compare(entityIdSource, o.entityIdSource).
												compare(entityTypeSource, o.entityTypeSource)
										.compare(entityIdDestination, o.entityIdDestination).
											    compare(entityTypeDestination, o.entityTypeDestination).result();
	}
	public String getEntityIdSource() {
		return entityIdSource;
	}
	public void setEntityIdSource(String entityIdSource) {
		this.entityIdSource = entityIdSource;
	}
	public String getEntityTypeSource() {
		return entityTypeSource;
	}
	public void setEntityTypeSource(String entityTypeSource) {
		this.entityTypeSource = entityTypeSource;
	}
	public String getEntityIdDestination() {
		return entityIdDestination;
	}
	public void setEntityIdDestination(String entityIdDestination) {
		this.entityIdDestination = entityIdDestination;
	}
	public String getEntityTypeDestination() {
		return entityTypeDestination;
	}
	public void setEntityTypeDestination(String entityTypeDestination) {
		this.entityTypeDestination = entityTypeDestination;
	}
	

}
