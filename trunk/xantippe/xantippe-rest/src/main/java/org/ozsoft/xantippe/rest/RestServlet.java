package org.ozsoft.xantippe.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ozsoft.xantippe.Database;
import org.ozsoft.xantippe.XmldbException;

/**
 * HTTP servlet handling WebDAV requests.
 * 
 * Only basic WebDAV level 1 operations are supported.
 * 
 * @author Oscar Stigter
 */
public class RestServlet extends HttpServlet {

	/** Serial version UID. */
	private static final long serialVersionUID = 1L;

	/** The log. */
	private static final Log LOG = LogFactory.getLog(RestServlet.class);
	
	/** The servlet context. */
	private final String context;
	
	/** The database. */
	private final Database database;
	
	/**
	 * Constructor.
	 * 
	 * @param database
	 *            The database.
	 */
	public RestServlet(String context, Database database) {
		if (context == null) {
			throw new IllegalArgumentException("Null context");
		}
        if (database == null) {
            throw new IllegalArgumentException("Null database");
        }
        this.context = context;
		this.database = database;
		LOG.debug("Started");
	}
	
	/**
	 * Starts the servlet.
	 */
	public void start() throws XmldbException {
	    LOG.debug("Starting");
        database.start();
        LOG.debug("Started");
    }

    /**
     * Shuts down the servlet.
     */
	public void shutdown() throws XmldbException {
        LOG.debug("Shutting down");
        database.shutdown();
        LOG.debug("Shut down");
	}

    /**
     * Handles an HTTP request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void service(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String method = request.getMethod();
		LOG.debug("Method: " + method);
//		for (Enumeration e = request.getHeaderNames(); e.hasMoreElements();) {
//			String name = (String) e.nextElement();
//			String value = request.getHeader(name);
//			LOG.debug("Header: " + name + ": " + value);
//		}
		if (method.equals("OPTIONS")) {
			doOptions(request, response);
		} else if (method.equals("HEAD")) {
			doHead(request, response);
		} else if (method.equals("GET")) {
			doGet(request, response);
		} else if (method.equals("PUT")) {
			doPut(request, response);
		} else if (method.equals("DELETE")) {
			doDelete(request, response);
		} else if (method.equals("POST")) {
			doPost(request, response);
		} else {
			LOG.warn("Unsupported HTTP method: " + method);
			response.setStatus(400);
		}
	}

    /**
     * Handles an OPTIONS request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doOptions(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		LOG.debug("OPTIONS");
		response.setStatus(200);
		response.addHeader("Allow", "OPTIONS, HEAD, GET, PUT, DELETE, POST");
	}

    /**
     * Handles a HEAD request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doHead(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String uri = request.getPathInfo();
		LOG.debug("HEAD " + uri);
	}

    /**
     * Handles a GET request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String uri = getUriFromRequest(request);
		LOG.debug("GET " + uri);
	}

    /**
     * Handles a PUT request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
	        throws ServletException, IOException {
		String uri = getUriFromRequest(request);
		LOG.debug("PUT " + uri);
	}

    /**
     * Handles a DELETE request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
        String uri = getUriFromRequest(request);
		LOG.debug("DELETE " + uri);
	}
	
    /**
     * Handles a POST request.
     * 
     * @param request
     *            The request.
     * @param response
     *            The response.
     */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
        String uri = getUriFromRequest(request);
		LOG.debug("POST " + uri);
	}
	
    /**
     * Returns the request URI, trimming the servlet context.
     * 
     * @param request
     *            The request.
     * 
     * @return The request URI.
     */
    private String getUriFromRequest(HttpServletRequest request) {
        String uri = request.getRequestURI();
        if (context.length() != 0) {
            if (uri.startsWith(context)) {
                uri = uri.substring(context.length());
            }
        }
        if (uri.length() == 0) {
            uri = "/";
        }
        return uri;
    }

//    /**
//     * Returns the resource name from a resource URI.
//     * 
//     * @param uri
//     *            The resource URI.
//     * 
//     * @return The resource name.
//     */
//    private static String getResourceName(String uri) {
//        String name = uri;
//        if (name.length() > 1) {
//            int pos = name.lastIndexOf('/');
//            if (pos != -1) {
//                name = name.substring(pos + 1);
//            }
//        }
//        return name;
//    }
//    
//	/**
//	 * Returns the request body from an HTTP request.
//	 * 
//	 * @param request
//	 *            The request.
//	 * 
//	 * @return The request body.
//	 * 
//	 * @throws IOException
//	 *             If the request could not be read.
//	 */
//	private static String getRequestBody(HttpServletRequest request) throws IOException {
//		BufferedReader reader = request.getReader();
//		StringBuilder sb = new StringBuilder();
//		String line = null;
//		while ((line = reader.readLine()) != null) {
//			sb.append(line).append('\n');
//		}
//		String requestBody = sb.toString();
//		if (requestBody.length() != 0) {
//			LOG.debug("Request body:\n" + requestBody);
//		}
//		return requestBody;
//	}
	
}
