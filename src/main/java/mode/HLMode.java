package mode;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;

import eps.EPLFactory;
import eps.EsperHandler;
import eps.EsperStatements;
import eventsocket.Event;

public class HLMode implements Mode {

	@Override
	public void addStatements(EPAdministrator cepAdm, boolean useDefault) {
		
		EPLFactory.parseHLEvents(cepAdm, "./hl/events.txt");
		
		if (useDefault)
			EsperStatements.parseStatements(cepAdm, EsperHandler.RETENTION_TIME);
		else 
			EsperStatements.defaultStatementsHighLevel(cepAdm, EsperHandler.RETENTION_TIME);	

	}
	
	@Override
	public String toString() {
		return "High-level mode";
	}

	@Override
	public void addEventTypes(Configuration cepConfig) {		
	    cepConfig.addEventType("Event", Event.class.getName());		
	}

}
