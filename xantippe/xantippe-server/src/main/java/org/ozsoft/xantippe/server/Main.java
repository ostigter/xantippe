package org.ozsoft.xantippe.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.ozsoft.xantippe.Database;
import org.ozsoft.xantippe.DatabaseImpl;
import org.ozsoft.xantippe.XmldbException;
import org.ozsoft.xantippe.rest.RestServlet;

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
	
	/** Port to listen to. */
	private static final int PORT = 8088;

	/** Servlet context. */
	private static final String CONTEXT = "/xantippe";
	
    /** Database data directory. */
    private static final String DATA_DIR = "target/test-data";
  
    /** Log. */
    private static final Log LOG = LogFactory.getLog(Main.class);
    
    private static RestServlet servlet;
    
	/**
	 * The application's entry point.
	 * 
	 * @param args
	 *            Command line arguments.
	 */
	public static void main(String[] args) {
        try {
            // Start REST servlet with a running database.
            Database database = new DatabaseImpl();
            database.setDatabaseLocation(DATA_DIR);
            servlet = new RestServlet(CONTEXT + "/rest", database);
            servlet.start();
            
            registerShutdownHook();
            
            // Start Jetty server.
            Server server = new Server(PORT);
            ServletHandler handler = new ServletHandler();
            handler.addServletWithMapping(new ServletHolder(servlet), CONTEXT + "/rest/*");
            server.setHandler(handler);
            server.start();
            LOG.info(String.format("Server started, listening on port %d", PORT));
            
        } catch (XmldbException e) {
            LOG.error("Could not start REST servlet", e);
        } catch (Exception e) {
            LOG.error("Could not start Jetty server", e);
        }
        
	}

    private static void shutdown() {
        LOG.debug("Shutting down");
        
        try {
            LOG.warn("JVM shutdown detected; shutting down server");
            if (servlet != null) {
                servlet.shutdown();
            }
        } catch (XmldbException e) {
            LOG.error("Could not properly shutdown servlet", e);
        }
        
        LOG.info("Server shut down");
    }
    
    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.warn("JVM shutdown detected; shutting down server");
                shutdown();
           }
        });
    }
    
}
