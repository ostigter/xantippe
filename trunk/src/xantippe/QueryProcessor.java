package xantippe;


import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.trans.XPathException;

import org.apache.log4j.Logger;


/**
 * Abstract base class for an XQuery processor.
 * 
 * @author Oscar Stigter
 */
public class QueryProcessor {


	/** log4j logger. */
	protected static final Logger logger =
			Logger.getLogger(QueryProcessor.class);
	
	/** Back-reference to the database. */
	private final DatabaseImpl database;

	/** Saxon configuration. */
	private final Configuration config;
	
	/** Static query context. */
	private final StaticQueryContext staticQueryContext;
	
	/** Dynamic query context. */
	private final DynamicQueryContext dynamicQueryContext;
	
	
    //------------------------------------------------------------------------
    //  Constructor
    //------------------------------------------------------------------------

    
	/**
	 * Constructor.
	 */
	QueryProcessor(DatabaseImpl database) {
		super();
		
		this.database = database;
		
		config = new Configuration();
		config.setURIResolver(new XmldbURIResolver());
		staticQueryContext = new StaticQueryContext(config);
        dynamicQueryContext = new DynamicQueryContext(config);
	}
	
	
    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------

    
    /* package */ OutputStream executeQuery(String query)
    		throws XmldbException {
    	if (query == null || query.length() == 0) {
    		throw new IllegalArgumentException("Null or empty query");
    	}
    	
//        logger.debug("Executing query: \n" + query);
        
        try {
            XQueryExpression expr = staticQueryContext.compileQuery(query);
            dynamicQueryContext.clearParameters();
            OutputStream os = new ByteArrayOutputStream();
            Result result = new StreamResult(os);
            expr.run(dynamicQueryContext, result, null);
//            logger.debug("Query result:\n" + os.toString());
            return os;
            
        } catch (XPathException e) {
            throw new XmldbException (
            		"Error executing query: " + e.getMessage());
        }
    }
    
    
    //------------------------------------------------------------------------
    //  Inner classes
    //------------------------------------------------------------------------

    
    private class XmldbURIResolver implements URIResolver {


    	public Source resolve(String uri, String baseUri)
    			throws TransformerException {
    		logger.debug("Resolve URI '" + uri + "'");
    		
    		Source source = null;
    		
    		try {
	    		Document doc = database.getDocument(uri);
	    		if (doc != null) {
	    			source = new StreamSource(doc.getContent());
	    		}
    		} catch (XmldbException e) {
    			// Already handled; ignore.
    		}
    		
    		return source;
		}
    	
    }
    
    
}
