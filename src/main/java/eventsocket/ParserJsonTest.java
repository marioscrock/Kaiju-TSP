package eventsocket;

import com.google.gson.Gson;

/**
 * Simple class to test {@link eventsocket.ParserJson}.
 * @author Mario
 *
 */
public class ParserJsonTest {
	
	public static void main(String[] args) throws InterruptedException {
		
		String s1 = "{\n" + 
				"    \"fields\": {\n" + 
				"        \"field_1\": 30,\n" + 
				"        \"field_2\": 4,\n" + 
				"        \"field_N\": 59,\n" + 
				"        \"n_images\": 660\n" + 
				"    },\n" + 
				"    \"name\": \"docker\",\n" + 
				"    \"tags\": {\n" + 
				"        \"host\": \"raynor\"\n" + 
				"    },\n" + 
				"    \"timestamp\": 1458229140\n" + 
				"}";
		
		String s2 = " {\n" + 
				"    \"metrics\": [\n" + 
				"        {\n" + 
				"            \"fields\": {\n" + 
				"                \"field_1\": 30,\n" + 
				"                \"field_2\": 4,\n" + 
				"                \"field_N\": 59,\n" + 
				"                \"n_images\": 660\n" + 
				"            },\n" + 
				"            \"name\": \"docker\",\n" + 
				"            \"tags\": {\n" + 
				"                \"host\": \"raynor\"\n" + 
				"            },\n" + 
				"            \"timestamp\": 1458229140\n" + 
				"        },\n" + 
				"        {\n" + 
				"            \"fields\": {\n" + 
				"                \"field_1\": 30,\n" + 
				"                \"field_2\": 4,\n" + 
				"                \"field_N\": 59,\n" + 
				"                \"n_images\": 660\n" + 
				"            },\n" + 
				"            \"name\": \"docker\",\n" + 
				"            \"tags\": {\n" + 
				"                \"host\": \"raynor\"\n" + 
				"            },\n" + 
				"            \"timestamp\": 1458229140\n" + 
				"        }\n" + 
				"    ]\n" + 
				"}";
		
		String s3 = " {\n" + 
				"    \"events\": [\n" + 
				"        {\n" + 
				"           \"timestamp\": 1458229140,\n" + 
				"            \"event\": {\n" + 
				"                \"event\": \"Commit\",\n" + 
				"                \"commit_msg\": \"Fix connection pool\"\n" + 
				"            }\n" + 
				"		},\n" +
				"      {\n" + 
				"           \"timestamp\": 1458249140,\n" + 
				"            \"event\": {\n" + 
				"                \"event\": \"Alert_CPU\",\n" + 
				"                \"alert_msg\": \"HighCPUUsage\"\n" + 

				"            }\n" + 
				"      }\n" + 
				"    ]\n" + 
				"}";
		
		String s4 = " {\n" +
				"		\"timestamp\": 1458229140,\n" + 
				"       \"event\": {\n" + 
				"       	\"event\": \"Commit\",\n" + 
				"           \"commit_msg\": \"Fix connection pool\"\n" + 
				"      	}\n" + 
				"}";
		
		Gson gson = new Gson();
		
		System.out.println(gson.fromJson(s1, Metric.class));
		System.out.println(gson.fromJson(s2, Metric.class));
		System.out.println(gson.fromJson(s3, Event.class));
		System.out.println(gson.fromJson(s4, Event.class));

	}

}
