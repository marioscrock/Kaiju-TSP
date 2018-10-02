package collector;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordCollector {
	
	private final int BUFFER_SIZE;
	
	private Set<String[]> dataToWrite;
	private String filepath;
	private AtomicInteger numbRecords; 
	
	
	public RecordCollector (String filepath, int bufferSize) {	
		dataToWrite = ConcurrentHashMap.newKeySet();
		numbRecords = new AtomicInteger(0);
		this.BUFFER_SIZE = bufferSize;
		this.filepath = filepath;
		
		//Empty file
		saveData(false);
	}
	
	public void addRecord(String[] record) {
		dataToWrite.add(record);
		numbRecords.getAndIncrement();
		
		//Save data to file
		synchronized(numbRecords) {
			if (numbRecords.get() > BUFFER_SIZE) {
				
				System.out.println("Saving " + filepath + " " + numbRecords.get() + "records");
				numbRecords.set(0);
				saveData(true);	
				
			}
		}
	}
	
	public void saveData(boolean append) {
		
        FileWriter pw;
		try {
			pw = new FileWriter(filepath, append);
        
	        synchronized (dataToWrite) {
		        Iterator<String[]> s = dataToWrite.iterator();
		        while(s.hasNext()){      	       		
		        		String[] current  = s.next();
		        		
		        		if (current.length > 0) {
		        			for (int i = 0; i < current.length - 1; i++) {
		        				String str = current[i];
		        				pw.append(str);
		        				pw.append(",");
		        			}
		        			String str = current[current.length - 1];
		        			pw.append(str);
		                    pw.append("\n");            
		            	}
		        		pw.flush();
		        		dataToWrite.remove(current);		
		        }
		        pw.close();
	        }
	        
        } catch (IOException e) {
			e.printStackTrace();
		}
            
    }
	
	
	
}
