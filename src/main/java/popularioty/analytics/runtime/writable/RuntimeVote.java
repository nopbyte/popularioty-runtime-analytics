package popularioty.analytics.runtime.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class RuntimeVote implements WritableComparable<RuntimeVote>{

	public static String SU_WEB_OBJECT= "SU_WO";
	
	public static String SU_EVENT = "SU_EVENT";
	
	public static String SU_NON_EVENT = "SU_NON_EVENT";
	
	public static String SU_DISCARD_JS = "SU_DISCARD_JS";
	
	public static String SU_DISCARD_POLICY = "SU_DISCARD_POLICY";
	
	public static String SU_DISCARD_FILTER = "SU_DISCARD_FILTER";

	//Choose from the constants above...
	private String typeOfVote;
	
	private int value;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		typeOfVote = in.readUTF();
		value= in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(typeOfVote);
		out.writeInt(value);
		
	}

	@Override
	public int compareTo(RuntimeVote o) {
		return 0;
	}
	
	
	public String getTypeOfVote() {
		return typeOfVote;
	}

	public RuntimeVote setTypeOfVote(String typeOfVote) {
		this.typeOfVote = typeOfVote;
		return this;
	}

	public int getValue() {
		return value;
	}

	public RuntimeVote setValue(int value) {
		this.value = value;
		return this;
	}


		

}
