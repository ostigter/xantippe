package xantippe;


import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Set;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.CollectionURIResolver;
import net.sf.saxon.Configuration;
import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.om.ArrayIterator;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.AnyURIValue;

import org.apache.log4j.Logger;


/**
 * XQuery processor powered by Saxon.
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
		config.setURIResolver(new DocumentURIResolver());
        config.setCollectionURIResolver(new XmldbCollectionURIResolver());
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

    
    /**
     * Document URI resolver.
     * 
     * Used by the XQuery fn:doc() function.
     * 
     * @author Oscar Stigter
     */
    private class DocumentURIResolver implements URIResolver {


        /**
         * Resolves a document URI and returns the document content.
         * 
         * @param  uri  the document URI
         * @param  baseUri  the base URI (always null)
         * 
         * @return  the document content
         */
        public Source resolve(String uri, String baseUri)
    			throws TransformerException {
    		logger.debug("Resolve document URI: '" + uri + "'");
    		
    		Source source = null;
    		
    		try {
	    		Document doc = database.getDocument(uri);
	    		try {
	    		    source = new StreamSource(doc.getContent());
	    		} catch (XmldbException e) {
	    		    logger.error(String.format(
	    		            "Error retrieving content of document '%s': %s",
	    		            doc, e.getMessage()));
	    		}
    		} catch (XmldbException e) {
    		    // Runtime query error (client side)
    		    logger.debug("Document not found: '" + uri + "'");
    		}
    		
    		return source;
		}
    	
    }
    
    
    //------------------------------------------------------------------------
    
    
    /**
     * Collection URI resolver.
     * 
     * @author Oscar Stigter
     */
    private class XmldbCollectionURIResolver implements CollectionURIResolver {


        private static final long serialVersionUID = 1L;
        

        /**
         * Resolves a collection URI.
         */
        public SequenceIterator resolve(
                String uri, String baseUri, XPathContext context)
                throws XPathException {
            logger.debug("Resolve collection URI: '" + uri + "'");
            
            SequenceIterator it = null;
            
            try {
                // Get documents from the collection. 
                Collection col  = database.getCollection(uri);
                Set<Document> docs = col.getDocuments();
                
                // Build Saxon specific iterator with document URI's.
                AnyURIValue[] anyUriValues = new AnyURIValue[docs.size()];
                int i = 0;
                for (Document doc : docs) {
                    anyUriValues[i++] = new AnyURIValue(doc.getUri());
                }
                it = new ArrayIterator(anyUriValues);
                
            } catch (XmldbException e) {
                // Runtime query error (client side)
                logger.debug("Collection not found: '" + uri + "'");
            }
            
            return it;
        }
        
        
    }


}
