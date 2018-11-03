package websocket;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

/**
 * Handler class for the web socket.
 * @author Mario
 *
 */
@WebSocket
public class WebSocketHandler {
	
	/**
	 * Method to handle incoming connections.
	 * @param client
	 * @throws Exception
	 */
    @OnWebSocketConnect
    public void onConnect(Session client) throws Exception {
        JsonTracesWS.clientSet.add(client);
    }
    
    /**
     * Method to handle closing connections.
     * @param client
     * @param statusCode
     * @param reason
     */
    @OnWebSocketClose
    public void onClose(Session client, int statusCode, String reason) {
    	JsonTracesWS.clientSet.remove(client);
        JsonTracesWS.broadcastMessage("Bye!");
    }
    
    /**
     * Method to handle incoming messages.
     * @param user
     * @param message
     */
    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        JsonTracesWS.broadcastMessage("No one cares!");
    }
    
}
