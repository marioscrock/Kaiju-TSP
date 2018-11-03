package eventsocket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable class exposing a socket on port {@code 9876} accepting {@link eventSocket.Metric Metric} and
 * {@link eventSocket.Event Event} event in JSON format to be sent the Esper engine.
 * @author Mario
 *
 */
public class EventSocket implements Runnable {
	
	private final static Logger log = LoggerFactory.getLogger(EventSocket.class);
	
	/**
	 * Run implementation for the EventSocket class.
	 */
	@Override
	public void run() {

		ServerSocket serverSocket = null;
		
		try {
			serverSocket = new ServerSocket(9876);
			Socket clientSocket = serverSocket.accept();
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			log.info("Starting socket");
			
			String line;
			while ((line = inputReader.readLine()) != null) {
				collector.Collector.executor.execute(new ParserJson(line));
			}
			
		} catch (IOException e) {
			log.info(e.getClass().getSimpleName());
			log.info(e.getMessage());
		} finally {
			try {
				serverSocket.close();
		    	Thread eventSocketThread = new Thread(this);
		    	eventSocketThread.start();
			} catch (IOException e) {
				log.info(e.getClass().getSimpleName());
				log.info(e.getMessage());
			}
		}
	}   

}
