package collector;

import websocket.JsonTraces;

public class Try {

	public static void main(String[] args) {
		Thread webSocketThread = new Thread(new JsonTraces());
    	webSocketThread.run();
	}

}
