package com.diffeo.kvlayer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

























import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
// try 'jackson'
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;


interface ClientHandler extends Runnable {
	void init(Socket client, RpcHandler logic);
}
interface RpcHandler {
	Object handle(String method, List<Object> params) throws Exception;
}
class EchoHandler implements RpcHandler {
	public Object handle(String method, List<Object> params) {
		System.err.printf("got method \"%s\"\n", method);
		return "echo: " + method;
	}
}

/**
 * This Server object holds the listening ServerSocket that accepts connections.
 * It creates a new Thread for each client Socket and runs the protocol in a new ClientHandler object.
 * @author bolson
 *
 */
public class CborRpcServer {
	private static final Logger logger = LogManager.getLogger("com.diffeo.kvlayer.CborRpcServer");
	int port = 7321;
	RpcHandler logic;
	Class handlerClass = null;
	int clientNum = 0;
	public CborRpcServer() {
	}
	public CborRpcServer(int port) {
		this.port = port;
	}
	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg == null) {continue;}
			if (arg.startsWith("--port=")) {
				port = Integer.parseInt(arg.substring(7));
			} else {
				logger.warn("unknown arg: " + arg);
			}
		}
	}
	public void run() throws InstantiationException, IllegalAccessException {
		ServerSocket sock = null;
		try {
			sock = new ServerSocket(port);
			sock.setReuseAddress(true);
			while (true) {
				Socket client = sock.accept();
				ClientHandler handler;
				if (handlerClass != null) {
					handler = (ClientHandler) handlerClass.newInstance();
				} else {
					handler = new CborClientHandler();
				}
				handler.init(client, logic);
				Thread t = new Thread(null, handler, "Client" + clientNum);
				t.start();
				clientNum++;
			}
		} catch (IOException e) {
			logger.error("server socket failed", e);
			e.printStackTrace();
		}
		if (sock != null) {
			try {
				sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		CborRpcServer server = new CborRpcServer();
		server.logic = new EchoHandler();
		server.parseArgs(args);
		try {
			server.run();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
}

class CborRpcMessage {
	public Object id;
	
	public CborRpcMessage() {}
}

class CborRpcRequest extends CborRpcMessage {
	public String method;
	public List<Object> params;

	public CborRpcRequest() {}
	public CborRpcRequest(Object id, String method, List<Object> params) {
		this.id = id;
		this.method = method;
		this.params = params;
	}
}

class CborRpcResponse extends CborRpcMessage {
	public Object result;
	
	public CborRpcResponse() {};
	public CborRpcResponse(Object id, Object retval) {
		this.id = id;
		this.result = retval;
	}
}

class CborRpcError extends CborRpcMessage {
	public String error;
	
	public CborRpcError() {}
	public CborRpcError(Object id, String error) {
		this.id = id;
		this.error = error;
	}
}

/**
 * Handle a client connection Socket and do CBOR RPC protocol.
 * Call out to logic in a RpcHandler object to actually do API things.
 * @author bolson
 *
 */
class CborClientHandler implements ClientHandler, Runnable {
	private static final Logger logger = LogManager.getLogger("com.diffeo.kvlayer.CborClientHandler");
	protected Socket client;
	protected RpcHandler logic;

	public CborClientHandler() {}
	
	public void init(Socket client, RpcHandler logic) {
		this.client = client;
		this.logic = logic;
	}
	
	/**
	 * Run the CBOR RPC protocol until the client Socket disconnects.
	 */
	public void run() {
		InputStream rawIns;
		InputStream ins;
		logger.info("handler starting " + this);
		try {
			rawIns = client.getInputStream();
			ins = new BufferedInputStream(rawIns);
			OutputStream outs = null;
			CBORFactory cborf = new CBORFactory();
			// Don't close the input after reading one object.
	        cborf.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
	        ObjectMapper mapper = new ObjectMapper(cborf);
	        ObjectReader reader = mapper.reader(CborRpcRequest.class);
	        while (true) {
		        CborRpcRequest message = reader.readValue(ins);
		        if (outs == null) {
		        	outs = client.getOutputStream();
		        }
		        CborRpcMessage retval = handleMessage(message);
		        if (outs == null) {
		        	outs = client.getOutputStream();
		        }
		        // the jackson methods that write to OuptutStreams have a bad habit of calling .close() on them.
		        outs.write(mapper.writeValueAsBytes(retval));
	        }
		} catch (JsonMappingException jme) {
			if (jme.getMessage().contains("end-of-input")) {
				logger.info("end of input " + this);
			} else {
				logger.error("uknonwn jme", jme);
			}
		} catch (IOException e) {
			logger.error("failure in client socket", e);
		}
		if (!this.client.isClosed()) {
			try {
				this.client.close();
			} catch (IOException e) {
				logger.info("failure on client close " + this, e);
			}
		}
		this.close();
		this.client = null;
		this.logic = null;
		logger.info("handler exiting " + this);
	}
	
	/**
	 * Close out object state after connection is gone.
	 * Nothing in CborClientHandler, mostly a hook for subclasses.
	 */
	protected void close() {}
	
	/**
	 * Translates from CBOR RPC messages to RpcHandler interface.
	 * Returns results in message, or catches exception and returns error message.
	 * @param message
	 * @return
	 */
	public CborRpcMessage handleMessage(CborRpcRequest message) {
		try {
			Object retval = logic.handle(message.method, message.params);
			return new CborRpcResponse(message.id, retval);
		} catch (Exception e) {
			logger.error("error handling message: " + message.method, e);
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			return new CborRpcError(message.id, sw.toString());
		}
	}
}
