package eventsocket;

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
				"            \"event\": {\n" + 
				"                \"event\": \"Commit\",\n" + 
				"                \"commit_msg\": \"Fix connection pool\"\n" + 
				"            }\n" + 
				"		},\n" +
				"      {\n" + 
				"            \"event\": {\n" + 
				"                \"event\": \"Alert_CPU\",\n" + 
				"                \"alert_msg\": \"HighCPUUsage\"\n" + 

				"            }\n" + 
				"      }\n" + 
				"    ]\n" + 
				"}";
		
		System.out.println(s1);
		System.out.println(s2);
		System.out.println(s3);
		
//		Thread t1 = new Thread(new ParserJson(s1));
//		Thread t2 = new Thread(new ParserJson(s2));
//		Thread t3 = new Thread(new ParserJson(s3));
//		t1.start();
//		t2.start();
//		t3.start();
//		t1.join();
//		t2.join();
//		t3.join();

	}

}
