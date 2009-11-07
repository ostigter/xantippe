package org.ozsoft.xantippe.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;

/**
 * Standalone server.
 * 
 * Uses an embedded Jetty.
 * 
 * Connection URL: "http://localhost:8088/xantippe/".
 * 
 * @author Oscar Stigter
 */
public class Main {
	
	/** The log. */
	private static final Log LOG = LogFactory.getLog(Main.class);
	
	/** The port to listen to. */
	private static final int PORT = 8088;

//	/** The servlet context. */
//	private static final String CONTEXT = "/xantippe";
	
	/**
	 * The application's entry point.
	 * 
	 * @param args
	 *            Command line arguments.
	 */
	public static void main(String[] args) {
//	    Database database = new DatabaseImpl();
		Server server = new Server(PORT);
		ServletHandler handler = new ServletHandler();
//		handler.addServletWithMapping(new ServletHolder(servlet), CONTEXT + "/*");
		server.setHandler(handler);
		try {
			server.start();
			LOG.info(String.format("Started, listening on port %d", PORT));
		} catch (Exception e) {
			LOG.error("Could not start server", e);
		}
	}

}
