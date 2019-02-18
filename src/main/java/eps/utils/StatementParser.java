package eps.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListener;

public class StatementParser {
	
	private final static Logger log = LoggerFactory.getLogger(StatementParser.class);
	
	public static void parseStatements(EPAdministrator cepAdm, String filepath, String retentionTime) {
		
		List<String> replaced = new ArrayList<String>();
		
		try (Stream<String> lines = Files.lines(Paths.get(filepath))) {
			   replaced = lines
			       .map(line -> line.replaceAll(":retentionTime:", retentionTime))
			       .collect(Collectors.toList());
		} catch (IOException e) {
			log.error("Error in reading statements from file: " + e.getMessage());
			return;
		}
		
		try {
			for (String s : replaced) {
				
				String[] s_array = s.split("=", 2);
				String s_stmt = s_array[1];
				
				//Debug statements installed
				log.info(s_stmt);
				
				EPStatement stmt = cepAdm.createEPL(s_stmt);
				
				String[] prefix = s_array[0].split(",");
				
				Map<String, String> config = new HashMap<>();			
				for(String p : prefix) {
					config.put(p.split(":")[0], p.split(":")[1]);
				}	
				
				//Debug config of statements
				log.info(config.toString());
				
				if (config.get("listener") != null) {
					switch (config.get("listener")) {
					case "simple":
						stmt.addListener(new CEPListener(config.get("message")));
						break;
					default:
						break;
					}
				}			
			}					
		    		    
		} catch (Exception e) {
			log.error("Failed validating statements file: " + e.getMessage());
		}
		
	}

}
