package org.ozsoft.xantippe.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Main {
    
    /** Log. */
    private static final Log LOG = LogFactory.getLog(Main.class); 
    
    /** REST servlet. */
    private static final String SERVLET = "rest";
    
    private final HttpClient httpClient; 
    
    /** Servlet URI. */
    private String servletUri;
    
    public static void main(String[] args) {
        new Main();
    }
    
    public Main() {
        httpClient = new HttpClient();
        
        String connectionUri = "http://localhost:8088/xantippe";
        servletUri = String.format("%s/%s", connectionUri, SERVLET);
        
        String uri = servletUri + "/README.txt";
        
        put(uri, new File("README.txt"));

        get(uri);
    }
    
    private void get(String uri) {
        HttpMethod method = null;
        InputStream is = null;
        try {
            method = new GetMethod(uri);
            int statusCode = httpClient.executeMethod(method);
            is = method.getResponseBodyAsStream();
            if (is != null) {
                Reader reader = new InputStreamReader(is);
                StringBuilder sb = new StringBuilder();
                char[] buffer = new char[8192];
                int read = 0;
                try {
                    while ((read = reader.read(buffer)) > 0) {
                        sb.append(buffer, 0, read);
                    }
                } catch (IOException e) {
                    LOG.error("Could not read HTTP response", e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            LOG.error("Could not close stream", e);
                        }
                    }
                }
                LOG.debug("Response:\n" + sb.toString());
            }
            if (statusCode != HttpStatus.SC_OK) {
                LOG.error("HTTP error: " + method.getStatusLine());
            }               
        } catch (IOException e) {
            LOG.error("Could not execute GET method", e);
        } finally {
            if (method != null) {
                method.releaseConnection();
            }
        }
    }
    
    private void put(String uri, File file) {
        HttpMethod method = null;
        try {
            method = new PutMethod(uri);
            RequestEntity entity = new InputStreamRequestEntity(
                    new FileInputStream(file), "soap/xml; charset=utf-8");
            ((EntityEnclosingMethod) method).setRequestEntity(entity);
            int statusCode = httpClient.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK) {
                LOG.error(String.format("HTTP error: %s\nHTTP response:\n%s",
                        method.getStatusLine(), method.getResponseBodyAsString()));
            }               
        } catch (Exception e) {
            LOG.error("Could not execute PUT method", e);
        } finally {
            if (method != null) {
                method.releaseConnection();
            }
        }
    }

}
