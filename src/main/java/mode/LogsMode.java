package mode;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;

import eps.EsperHandler;
import eps.EsperStatements;
import eventsocket.Event;
import eventsocket.FLog;

public class LogsMode implements Mode {

	@Override
	public void addStatements(EPAdministrator cepAdm, boolean useDefault) {
		if (useDefault)
			EsperStatements.parseStatements(cepAdm, EsperHandler.RETENTION_TIME);
		else 
			EsperStatements.defaultStatementsLogs(cepAdm, EsperHandler.RETENTION_TIME);	
	}
		
	@Override
	public void addEventTypes(Configuration cepConfig) {
	    cepConfig.addEventType("Event", Event.class.getName());
	    cepConfig.addEventType("FLog", FLog.class.getName());	
	}
	
	@Override
	public String toString() {
		return "Logs mode";
	}

}
