package websocket;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

@WebSocket
public class WebSocketHandler {

    @OnWebSocketConnect
    public void onConnect(Session client) throws Exception {
        JsonTracesWS.clientSet.add(client);
        JsonTracesWS.broadcastMessage("Hello!");
    }

    @OnWebSocketClose
    public void onClose(Session client, int statusCode, String reason) {
    	JsonTracesWS.clientSet.remove(client);
        JsonTracesWS.broadcastMessage("Bye!");
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        JsonTracesWS.broadcastMessage("No one cares!");
    }
    
}
