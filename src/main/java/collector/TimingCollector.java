package collector;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.util.ConcurrentHashSet;

public class TimingCollector {
	
	private ConcurrentHashSet<String[]> dataToWrite;
	private String filepath;
	private AtomicInteger numbRecords = new AtomicInteger(0);
	
	public TimingCollector (String filepath) {
		dataToWrite = new ConcurrentHashSet<>();
		this.filepath = filepath;
	}
	
	public void addRecord(String[] record) {
		dataToWrite.add(record);
		numbRecords.incrementAndGet();
		
		System.out.println(numbRecords.get());
		//Save data to file
		if (numbRecords.get() > 200) {
			System.out.println("Saving");
			numbRecords.set(0);
			collector.Collector.executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						saveData();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			});		
		}
	}
	
	public void saveData() throws IOException{
		
		String fp = this.getClass().getResource(filepath).getPath();

        FileWriter pw = new FileWriter(fp, true);
        
        synchronized (dataToWrite) {
	        Iterator<String[]> s = dataToWrite.iterator();
	        if (s.hasNext()==false){
	            System.out.println("Empty");
	        }
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
            
    }
	
	
	
}
