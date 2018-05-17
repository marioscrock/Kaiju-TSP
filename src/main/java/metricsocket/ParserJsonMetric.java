package metricsocket;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParserJsonMetric implements Runnable {
	
	private String jsonToParse;
	
	public ParserJsonMetric(String readLine) {
		jsonToParse = readLine;
	}

	@Override
	public void run() {
		
		Gson gson = new Gson();
		
		JsonParser parser = new JsonParser();
		JsonObject jObj = (JsonObject) parser.parse(jsonToParse);
		
		if (jObj.get("metrics") == null){	
			
			//Metric fields are now a map String:String
			
			//try {
				MetricSocket.metrics.add(gson.fromJson(jsonToParse, Metric.class));
			//} catch (NumberFormatException e) {
			//	System.out.println("******************************************************************");
			//	System.out.println(jsonToParse);
			//}
			
		} else {
			
			Metric[] metricsParsed = gson.fromJson(jObj.get("metrics"), Metric[].class);
			for (int i = 0; i < metricsParsed.length; i++) {
				MetricSocket.metrics.add((Metric) metricsParsed[i]);
			}
			
		}
		
		System.out.println(MetricSocket.metrics.size());

	}

}
