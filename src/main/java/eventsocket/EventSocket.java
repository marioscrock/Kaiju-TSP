package eventsocket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventSocket implements Runnable {
	
	private final static Logger log = LoggerFactory.getLogger(EventSocket.class);
	
	@Override
	public void run() {

		ServerSocket serverSocket = null;
		BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    	ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3,
    			10000, TimeUnit.MILLISECONDS, workQueue);
		
		try {
			serverSocket = new ServerSocket(9876);
			Socket clientSocket = serverSocket.accept();
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			log.info("Starting socket");
			
			String line;
			while ((line = inputReader.readLine()) != null) {
				executor.execute(new ParserJson(line));
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
