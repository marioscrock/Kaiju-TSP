package mode;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;

import eventsocket.EventSocketServer;

public interface Mode {
	
	default public void init() throws InterruptedException {
		//Open Events Socket
    	EventSocketServer es = new EventSocketServer();
    	Thread eventSocketThread = new Thread(es);
    	eventSocketThread.start();
	}
	
	public void addStatements(EPAdministrator cepAdm, boolean useDefault);
	
	public void addEventTypes(Configuration cepConfig);

}
