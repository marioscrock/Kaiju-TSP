package eps;

import thriftgen.*;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class EsperHandler {
	
	private EPRuntime cepRT;
	
	public void initializeHandler() {
		
		//The Configuration is meant only as an initialization-time object.
	    Configuration cepConfig = new Configuration();
	    
	    // We register thriftgen classes as objects the engine will have to handle
	    cepConfig.addEventType("Batch", Batch.class.getName());
	    cepConfig.addEventType("Log", Log.class.getName());
	    cepConfig.addEventType("Process", thriftgen.Process.class.getName());
	    cepConfig.addEventType("Span", Span.class.getName());
	    cepConfig.addEventType("SpanRef", SpanRef.class.getName());
	    cepConfig.addEventType("Tag", Tag.class.getName());
	 
	   // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine",cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    
	    // We register an EPL statement
	    EPAdministrator cepAdm = cep.getEPAdministrator();
	    EPStatement cepStatement = cepAdm.createEPL("select * from " +
	                                    "Span(operationName='GetDriver').win:length(3) " +
	                                    "having avg(duration) > 2");
	    cepStatement.addListener(new CEPListener());
	    
	    EPStatement cepStatement2 = cepAdm.createEPL("select * from " +
                "Tag(key='http.status_code') " +
                "having VLong=200");
	    cepStatement2.addListener(new CEPListener());
	    
	}
	
	public void sendBatch(Batch batch) {
		
		cepRT.sendEvent(batch);
		
		if(batch.getSpans() != null) {
			
			for(Span span : batch.getSpans()) {
				
				cepRT.sendEvent(span);
				
				if(span.getTags() != null) 
					for(Tag tag : span.getTags())
						cepRT.sendEvent(tag);
				
				if(span.getLogs() != null)
					for(Log log : span.getLogs()) {
						
						cepRT.sendEvent(log);
						
						if(log.getFields() != null) 
							for(Tag tag : log.getFields())
								cepRT.sendEvent(tag);	
						
					}

				if(span.getReferences() != null)
					for(SpanRef spanRef : span.getReferences())
						cepRT.sendEvent(spanRef);
				
			}
		}
		
		if(batch.getProcess() != null) {
			
			cepRT.sendEvent(batch.getProcess());
			
			if(batch.getProcess().getTags() != null)
				for(Tag tag : batch.getProcess().getTags())
					cepRT.sendEvent(tag);
	
		}	
	}

}
