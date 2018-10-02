package eventsocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.lang.Exception;
import eps.EsperHandler;

public class ParserJson implements Runnable {
	
	private final static Logger log = LoggerFactory.getLogger(ParserJson.class);
	private String jsonToParse;
	
	public ParserJson(String readLine) {
		jsonToParse = readLine;
	}

	@Override
	public void run() {
		
		Gson gson = new Gson();
		
		JsonParser parser = new JsonParser();
		
		try {
			
			JsonObject jObj = (JsonObject) parser.parse(jsonToParse);
		
			if (jObj.get("metrics") == null){	
				
				if(jObj.get("events") == null) {
					//Metric fields are now a map String:String
					EsperHandler.sendMetric(gson.fromJson(jsonToParse, Metric.class));
				} else {
					Event[] eventsParsed = gson.fromJson(jObj.get("events"), Event[].class);
					for (int i = 0; i < eventsParsed.length; i++) {
						EsperHandler.sendEvent((Event) eventsParsed[i]);
					}
				}
				
			} else {
				
				Metric[] metricsParsed = gson.fromJson(jObj.get("metrics"), Metric[].class);
				for (int i = 0; i < metricsParsed.length; i++) {
					EsperHandler.sendMetric((Metric) metricsParsed[i]);
				}
			}
		} catch (Exception e){
			log.info("Error parsing json " + jsonToParse + " | LINE DISCARDED");
			log.info(e.getClass().getSimpleName());
			log.info(e.getMessage());
			
		}

	}

}
