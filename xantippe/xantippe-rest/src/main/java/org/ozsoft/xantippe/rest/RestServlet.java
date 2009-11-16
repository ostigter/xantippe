package org.ozsoft.xantippe.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ozsoft.xantippe.Collection;
import org.ozsoft.xantippe.Database;
import org.ozsoft.xantippe.Document;
import org.ozsoft.xantippe.MediaType;
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

    /** XML content type. */
    private static final String XML_CONTENT_TYPE = "text/xml;charset=UTF-8";
    
    /** XML declaration. */
	private static final String XML_DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    
    /** Namespace. */
    private static final String NAMESPACE = "http://www.ozsoft.org/xantippe";
    
    /** Buffer size in bytes. */
    private static final int BUFFER_SIZE = 8192;
    
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
     * 
     * @throws XmldbException
     *             If the database could not be started.
     */
	public void start() throws XmldbException {
	    LOG.debug("Starting");
        database.start();
        LOG.debug("Started");
    }

    /**
     * Shuts down the servlet.
     * 
     * @throws XmldbException
     *             If the database could not be shutdown properly.
     */
	public void shutdown() throws XmldbException {
        LOG.debug("Shutting down");
        database.shutdown();
        LOG.debug("Shut down");
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#service(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
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
			response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doHead(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doHead(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String uri = request.getPathInfo();
		LOG.debug("HEAD " + uri);
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String uri = getUriFromRequest(request);
		LOG.debug("GET " + uri);
        if (database.exists(uri)) {
            try {
                if (database.isCollection(uri)) {
                    Collection col = database.getCollection(uri);
                    response.setContentType(XML_CONTENT_TYPE);
                    StringBuilder sb = new StringBuilder();
                    sb.append(XML_DECLARATION);
                    String validationMode = col.getValidationMode(false).name();
                    String compressionMode = col.getCompressionMode(false).name();
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
                    String created = df.format(new Date(col.getCreated()));
                    String modified = df.format(new Date(col.getModified()));
                    sb.append(String.format(
                            "<collection xmlns=\"%s\" created=\"%s\" modified=\"%s\" validation=\"%s\" compression=\"%s\">\n",
                            NAMESPACE, created, modified, validationMode, compressionMode));
                    sb.append("</collection>\n");
                    response.getWriter().write(sb.toString());
                } else {
                    Document doc = database.getDocument(uri);
                    MediaType mediaType = doc.getMediaType();
                    if (mediaType == MediaType.XQUERY) {
                        //TODO: Execute query and return the result.
                        //...
                    } else {
                        // Write document content as-is.
                        response.setContentType(mediaType.getContentType());
                        readDocumentContent(doc, response);
                    }
                }
            } catch (XmldbException e) {
                String msg = String.format("Resource could not be retrieved from the database: '%s'", uri); 
                LOG.warn(msg);
                sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
            }
        } else {
            String msg = String.format("Resource not found: '%s'", uri); 
            LOG.warn(msg);
            sendError(response, HttpServletResponse.SC_NOT_FOUND, msg);
        }
	}
	
	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
	        throws ServletException, IOException {
		String uri = getUriFromRequest(request);
		LOG.debug("PUT " + uri);
		if (database.exists(uri)) {
		    if (database.isDocument(uri)) {
	            // Overwrite existing document.
	            try {
	                Document doc = database.getDocument(uri);
                    writeDocumentContent(doc, request);
	            } catch (XmldbException e) {
	                String msg = String.format("Database I/O error for document '%s'", uri);
	                LOG.error(msg, e);
	                sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
	            }
		    } else {
                String msg = String.format("Method PUT not allowed for collection '%s'", uri);
                LOG.warn(msg);
                sendError(response, HttpServletResponse.SC_BAD_REQUEST, msg);
		    }
		} else {
		    // Create new document.
		    int p = uri.lastIndexOf('/');
		    String colUri = uri.substring(0, p);
		    if (colUri.length() == 0) {
		        // Special case for root collection.
		        colUri = "/";
		    }
		    if (database.isCollection(colUri)) {
		        try {
                    Collection col = database.getCollection(colUri);
    	            String docName = uri.substring(p + 1);
    	            Document doc = col.createDocument(docName);
    	            writeDocumentContent(doc, request);
		        } catch (XmldbException e) {
                    String msg = String.format("Database I/O error for document '%s'", uri);
                    LOG.error(msg, e);
                    sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
		        }
		    } else {
                String msg = String.format("Collection '%s' does not exist", uri);
                LOG.warn(msg);
                sendError(response, HttpServletResponse.SC_BAD_REQUEST, msg);
		    }
		}
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doDelete(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
        String uri = getUriFromRequest(request);
		LOG.debug("DELETE " + uri);
        if (database.exists(uri)) {
            try {
                database.delete(uri);
            } catch (XmldbException e) {
                String msg = String.format("Could not delete resource '%s'", uri);
                LOG.error(msg);
                sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
            }
        } else {
            String msg = String.format("Resource '%s' does not exist", uri);
            LOG.warn(msg);
            sendError(response, HttpServletResponse.SC_NOT_FOUND, msg);
        }
	}
	
	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
        String uri = getUriFromRequest(request);
		LOG.debug("POST " + uri);
	}
	
    /**
     * Writes the content of a stored document to the HTTP response.
     * 
     * @param doc
     *            The document.
     * @param response
     *            The HTTP response.
     * 
     * @throws XmldbException
     *             If the document content could not be read.
     * @throws IOException
     *             If the HTTP response could not be written.
     */
	private static void readDocumentContent(Document doc, HttpServletResponse response)
            throws XmldbException, IOException {
        InputStream is = doc.getContent();
        OutputStream os = response.getOutputStream();
        byte[] buffer = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = is.read(buffer)) > 0) {
            os.write(buffer, 0, read);
        }
        is.close();
	}
	
    /**
     * Writes the HTTP request body as the content of a stored document.
     * 
     * @param doc
     *            The document.
     * @param request
     *            The HTTP request.
     * 
     * @throws XmldbException
     *             If the document content could not be written.
     * @throws IOException
     *             If the request body could not read.
     */
	private static void writeDocumentContent(Document doc, HttpServletRequest request)
	        throws XmldbException, IOException {
        InputStream is = request.getInputStream();
        OutputStream os = doc.setContent();
        byte[] buffer = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = is.read(buffer)) > 0) {
            os.write(buffer, 0, read);
        }
        is.close();
	}
	
    /**
     * Sends an error response.
     * 
     * @param response
     *            The response.
     * @param status
     *            The status.
     * @param message
     *            The error message.
     * 
     * @throws IOException
     *             If the response could not be sent.
     */
	private void sendError(HttpServletResponse response, int status, String message)
	        throws IOException {
	    response.setStatus(status);
        StringBuilder sb = new StringBuilder();
        sb.append(XML_DECLARATION);
        sb.append(String.format("<error xmlns=\"%s\">\n", NAMESPACE));
        sb.append(String.format("  <message>%s</message>\n", message));
        sb.append("</error>\n");
        response.getWriter().write(sb.toString());
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
