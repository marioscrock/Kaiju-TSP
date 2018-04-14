package collector;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;

import thriftgen.Batch;

public class BatchSerialize {
	
	public static int minNumBatchToSerialize;
	public static List<String> strings = Collections.synchronizedList(new ArrayList<String>());

	public static void serialize(Batch batch, int numbBatches) {
		
		Gson gson = new Gson();
		
		if (numbBatches <= minNumBatchToSerialize) {
			
			synchronized (strings) {
				strings.add(gson.toJson(batch));
			}
		
		} else { 
			
	        PrintWriter file;
			try {
				
				file = new PrintWriter (new FileWriter("/Users/Mario/Desktop/dumpTraces.json"));
				
				StringBuilder b = new StringBuilder();
				b.append("[");
				
				synchronized (strings) {
					for (String s : strings) {
						b.append(s);
						b.append(",");
					}
				}
				
				b.deleteCharAt(b.toString().lastIndexOf(","));
				b.append("]");
				
				file.append(b.toString());
	            file.close();
	            
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
			
	}

}
