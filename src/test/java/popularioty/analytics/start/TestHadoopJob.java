package popularioty.analytics.start;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import popularioty.analytics.runtime.mappers.GenericEntityMapper;
import popularioty.analytics.runtime.mappers.GenericEntityReducer;
import popularioty.analytics.runtime.writable.RuntimeEdge;
import popularioty.analytics.runtime.writable.RuntimeKey;
import popularioty.analytics.runtime.writable.RuntimeVote;

/**
 * Unit test for simple App.
 */
public class TestHadoopJob 
{
	MapDriver<LongWritable, Text, RuntimeKey, RuntimeVote> mapDriver;
	  ReduceDriver<RuntimeKey,RuntimeVote , RuntimeKey, RuntimeEdge> reduceDriver;
	  MapReduceDriver<LongWritable, Text, RuntimeKey, RuntimeVote, RuntimeKey, RuntimeEdge> mapReduceDriver;
	 
	  @Before
	  public void setUp() {
	     GenericEntityMapper mapper = new GenericEntityMapper ();
	     GenericEntityReducer reducer = new GenericEntityReducer();
	     mapDriver = MapDriver.newMapDriver(mapper);
	     reduceDriver = ReduceDriver.newReduceDriver(reducer);
	     mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
	    /*mapDriver.withInput(new LongWritable(), new Text(
	    		//"src.soid=1426859511066689111ae7e5e46dca7066ec2bc59b3b5;owner.id=4f8c3900-36e5-4ec2-b44a-420ee773b632"));
	        "655209;1;796764372490213;804422938115889;6"));
	    mapDriver.withOutput(new Text("6"), new IntWritable(1));
	    mapDriver.runTest();*/
	  }
	 
	  @Test
	  public void testReducer() throws IOException {
	   /*List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("6"), values);
	    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    reduceDriver.runTest();*/
	  }
	   
	  /*@Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text(
	              "655209;1;796764372490213;804422938115889;6"));
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    mapReduceDriver.runTest();
	  }*/
}
