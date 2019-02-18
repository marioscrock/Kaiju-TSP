package mode;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;

import eps.EsperHandler;
import eps.EsperStatements;
import eps.utils.StatementParser;
import eventsocket.Event;
import eventsocket.Metric;

public class MetricsMode implements Mode {
	
	@Override
	public void addEventTypes(Configuration cepConfig) {
	    cepConfig.addEventType("Metric", Metric.class.getName());
	    cepConfig.addEventType("Event", Event.class.getName());	
	}
	
	@Override
	public void addStatements(EPAdministrator cepAdm, boolean parse) {			
		EsperStatements.defaultStatementsMetrics(cepAdm, EsperHandler.RETENTION_TIME);
		if (parse)
			StatementParser.parseStatements(cepAdm, "./stmts/statements.txt", EsperHandler.RETENTION_TIME);
		
		EsperStatements.reportHLEvents(cepAdm);
	}
	
	@Override
	public String toString() {
		return "Metrics mode";
	}
}
