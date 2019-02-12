package eventsocket;

import java.io.IOException;
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
public class EventSocketServer implements Runnable {
	
	public static int PORT = 9876;
	private final static Logger log = LoggerFactory.getLogger(EventSocketServer.class);
	
	/**
	 * Run implementation for the EventSocket class.
	 */
	@Override
	public void run() {
		
		ServerSocket serverSocket = null;
        Socket socket = null;

        try {
            serverSocket = new ServerSocket(PORT);
         
	        while (true) {
	            try {
	                socket = serverSocket.accept();
	            } catch (IOException e) {
	            	log.info("Exception " + e.getClass().getSimpleName() + ": " + e.getMessage());
	            } 
	            // new thread for a client
	            new EventSocketThread(socket).start();
	        }
        } catch (IOException e) {
			log.info("Exception " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }    
        finally {
        	try {
				socket.close();
			} catch (IOException e) {
				log.info("Exception " + e.getClass().getSimpleName() + ": " + e.getMessage());
			}
        }
        
	}   

}
