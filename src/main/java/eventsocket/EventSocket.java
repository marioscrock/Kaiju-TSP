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

public class EventSocket implements Runnable {
	
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
			
			System.out.println("Starting socket");
			
			while (true) {
				String line = inputReader.readLine();
				System.out.println(line);
				executor.execute(new ParserJson(line));
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				serverSocket.close();	
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}   

}
