package websocket;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;



@WebSocket
public class WebSocketHandler {

    @OnWebSocketConnect
    public void onConnect(Session client) throws Exception {
        JsonTraces.clientSet.add(client);
        JsonTraces.broadcastMessage("Hello!");
    }

    @OnWebSocketClose
    public void onClose(Session client, int statusCode, String reason) {
    	JsonTraces.clientSet.remove(client);
        JsonTraces.broadcastMessage("Bye!");
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        JsonTraces.broadcastMessage("No one cares!");
    }
    
}
