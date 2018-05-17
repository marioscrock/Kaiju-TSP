package metricsocket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MetricSocket {
	
	static public List<Metric> metrics = new ArrayList<>();
	
	public static void main(String[] args) throws InterruptedException {
		
		new Thread(new SimpleServer()).start();

	}

	static class SimpleServer implements Runnable {

		@Override
		public void run() {

			ServerSocket serverSocket = null;
			
			try {
				serverSocket = new ServerSocket(9876);
				Socket clientSocket = serverSocket.accept();
				BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				
				System.out.println("Starting socket");
				
				while (true) {
					new Thread(new ParserJsonMetric(inputReader.readLine())).start();
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
	    

}
