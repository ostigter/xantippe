package org.ozsoft.xantippe.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.ozsoft.xantippe.Database;
import org.ozsoft.xantippe.DatabaseImpl;

public class RestServletTest {
    
    /** Port to listen to. */
    private static final int PORT = 8089;

    /** Servlet context. */
    private static final String CONTEXT = "/xantippe";

    /** Database data directory. */
    private static final String DATA_DIR = "target/data-test";
  
    /** Log. */
    private static final Log LOG = LogFactory.getLog(RestServletTest.class);
    
    @Test
    public void test() {
        try {
            // Start REST servlet with a running database.
            Database database = new DatabaseImpl();
            database.setDatabaseLocation(DATA_DIR);
            RestServlet servlet = new RestServlet(CONTEXT, database);
            servlet.start();

            // Start Jetty server.
            Server server = new Server(PORT);
            ServletHandler handler = new ServletHandler();
            handler.addServletWithMapping(new ServletHolder(servlet), CONTEXT + "/*");
            server.setHandler(handler);
            server.start();
            LOG.debug("Server started");
            servlet.shutdown();
            server.stop();
            LOG.debug("Server shutdown");
            
        } catch (Exception e) {
            LOG.error("Could not start server", e);
        }
    }

}
