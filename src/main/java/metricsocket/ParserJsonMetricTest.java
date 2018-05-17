package metricsocket;

public class ParserJsonMetricTest {
	
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
		
		Thread t1 = new Thread(new ParserJsonMetric(s1));
		Thread t2 = new Thread(new ParserJsonMetric(s2));
		t1.start();
		t2.start();
		t1.join();
		t2.join();
		
		System.out.println(MetricSocket.metrics);

	}

}
