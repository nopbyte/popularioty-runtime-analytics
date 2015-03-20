package popularioty.analytics.example;

import java.util.HashMap;

import popularioty.analytics.example.WordCount.Map;
import popularioty.commons.services.search.FeedbackReputationSearch;
import popularioty.commons.services.searchengine.factory.SearchEngineFactory;
import popularioty.commons.services.searchengine.factory.SearchProvider;

public class ClassicSingleton {
	   private static ClassicSingleton instance = null;
	   public SearchProvider prov;
	   public FeedbackReputationSearch fedback;
	   public static java.util.Map<String,Object>  getSettings()
		{
			java.util.Map<String,Object>  settings = new HashMap<String,Object>();
			settings.put("storage.engine","couch_base");
			settings.put("couchbase.host","[\"192.168.56.105\"]");
			settings.put("couchbase.timeout.value","2");
			settings.put("couchbase.timeout.timeunit","2");		
			//settings.put("couchbase.port","8092");
			settings.put("feedback.bucket","feedback");
			settings.put("search.engine","elastic_search");
			settings.put("client.transport.host","192.168.56.105");
			settings.put("client.transport.port","9300");
			settings.put("index.aggregated","reputation_aggregations");
			settings.put("index.feedback","feedback");
			settings.put("index.metafeedback","meta_feedback");
			settings.put("index.subreputation","subreputation");
			
			return settings;
		}
	   
	   protected ClassicSingleton() {
	      // Exists only to defeat instantiation.
		   prov = SearchEngineFactory.getSearchProvider("elastic_search");
			if(prov==null)
				System.err.println("provider is null!");
			else{
			try {
				prov.init(getSettings());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			prov.toString();
			fedback = new FeedbackReputationSearch(Map.getSettings(), prov);
			}
			
	   }
	   public static ClassicSingleton getInstance() {
	      if(instance == null) {
	         instance = new ClassicSingleton();
	      }
	      return instance;
	   }

	@Override
	protected void finalize() throws Throwable {
		/*
		 * Hopefully we can close the ES connection propperly?
		 */
		prov.close(null);
	}
	   
	}
