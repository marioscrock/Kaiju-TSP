package mode;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;

import eps.EsperHandler;
import eps.EsperStatements;
import eventsocket.Event;
import eventsocket.Metric;

public class MetricsMode implements Mode {

	@Override
	public void addStatements(EPAdministrator cepAdm, boolean useDefault) {	
		if (useDefault)
			EsperStatements.parseStatements(cepAdm, EsperHandler.RETENTION_TIME);
		else 
			EsperStatements.defaultStatementsMetrics(cepAdm, EsperHandler.RETENTION_TIME);			
	}
		
	@Override
	public void addEventTypes(Configuration cepConfig) {
	    cepConfig.addEventType("Metric", Metric.class.getName());
	    cepConfig.addEventType("Event", Event.class.getName());	
	}
	
	@Override
	public String toString() {
		return "Metrics mode";
	}
}
