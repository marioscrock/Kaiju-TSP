package collector;

import websocket.JsonTracesWS;

public class Try {

	public static void main(String[] args) {
		Thread webSocketThread = new Thread(new JsonTracesWS());
    	webSocketThread.run();
	}

}
