// This file is part of the Xantippe XML database.
//
// Copyright 2008 Oscar Stigter
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ozsoft.xantippe;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

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
import net.sf.saxon.query.ModuleURIResolver;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.AnyURIValue;

import org.apache.log4j.Logger;

/**
 * XQuery processor powered by Saxon-B.
 * 
 * Saxon-B is an open source XQuery and XSLT processor by Michael H. Kay from
 * Saxonica (http://www.saxonica.com).
 * 
 * @author Oscar Stigter
 */
/* package */ class QueryProcessor {

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
	
	/* package */ QueryProcessor(DatabaseImpl database) {
		super();
		
		this.database = database;
		
		config = new Configuration();
		config.setURIResolver(new DocumentURIResolver());
        config.setCollectionURIResolver(new XmldbCollectionURIResolver());
        config.setModuleURIResolver(new XmldbModuleURIResolver());
		staticQueryContext = new StaticQueryContext(config);
        dynamicQueryContext = new DynamicQueryContext(config);
	}
    
	/**
	 * Executes the specified query.
	 * 
	 * @param  query  the query text
	 * 
	 * @return  an OutputStream with the plain text query result
	 * 
     * @throws  XmldbException
     *              if the query could not be executed
	 */
	public OutputStream executeQuery(String query)
    		throws XmldbException {
        logger.debug("Executing query: \n" + query);
        
        long startTime = System.currentTimeMillis();
        try {
            XQueryExpression expr = staticQueryContext.compileQuery(query);
            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Query compiled in " + duration + " ms");
            startTime = System.currentTimeMillis();
            dynamicQueryContext.clearParameters();
            OutputStream os = new ByteArrayOutputStream();
            Result result = new StreamResult(os);
            expr.run(dynamicQueryContext, result, null);
            duration = System.currentTimeMillis() - startTime;
            logger.debug("Query executed in " + duration + " ms");
            logger.debug("Query result:\n" + os.toString());
            return os;
            
        } catch (XPathException e) {
            throw new XmldbException (
            		"Error executing query: " + e.getMessage());
        }
    }
    
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
//    		logger.debug("Resolve document URI: '" + uri + "'");
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
        
    } // DocumentURIResolver
    
    /**
     * Collection URI resolver.
     * 
     * Used by the XQuery fn:collection() function.
     * 
     * @author Oscar Stigter
     */
    private class XmldbCollectionURIResolver implements CollectionURIResolver {

        private static final long serialVersionUID = 1L;

        /**
         * Returns the URI's of the documents stored in a specific collection.
         */
        public SequenceIterator resolve(String colUri, String baseUri, XPathContext context) throws XPathException {
            boolean recurse = false;
            
            // Parse query parameters
            int p = colUri.indexOf('?');
            if (p != -1) {
                String params = colUri.substring(p + 1);
                colUri = colUri.substring(0, p);
                for (String param : params.split(";")) {
//                    logger.debug("Query parameter: '" + param + "'");
                    p = param.indexOf('=');
                    if (p != -1) {
                        String name = param.substring(0, p);
                        String value = param.substring(p + 1);
                        if (name.equals("recurse")) {
                            recurse = value.equals("yes") ||
                                      value.equals("true"); 
                        }
                    }
                }
            }
            
            SequenceIterator it = null;
            
//            logger.debug("Resolve collection: '" + colUri + "'");
            
            try {
                // Get documents from collection. 
                Collection col  = database.getCollection(colUri);
                
                List<String> docUris = new ArrayList<String>();
                getDocumentUris(col, recurse, docUris);
                
                // Build Saxon specific iterator with document URI's.
                AnyURIValue[] anyUriValues = new AnyURIValue[docUris.size()];
                int i = 0;
                for (String docUri : docUris) {
                    anyUriValues[i++] = new AnyURIValue(docUri);
                }
                it = new ArrayIterator(anyUriValues);
                
            } catch (XmldbException e) {
                // Runtime query error (client side)
                logger.debug("Collection not found: '" + colUri + "'");
            }
            
            return it;
        }
        
        private void getDocumentUris(
                Collection col, boolean recurse, List<String> docUris) {
            for (Document doc : col.getDocuments()) {
                docUris.add(doc.getUri()); 
            }
            if (recurse) {
                for (Collection subCol : col.getCollections()) {
                    getDocumentUris(subCol, recurse, docUris);
                }
            }
        }
        
    } // CollectionURIResolver

    /**
     * Module URI resolver.
     * 
     * Used by the XQuery import statement.
     * 
     * @author Oscar Stigter
     */
    private class XmldbModuleURIResolver implements ModuleURIResolver {
        
        private static final long serialVersionUID = 1L;

        /**
         * Returns the contents of an XQuery module specified by its URI. 
         */
        public StreamSource[] resolve(
                String namespace, String baseUri, String[] locationHints)
                throws XPathException {
            logger.debug("Resolve module with namespace '" + namespace + "'");
            
            Document doc = null;
            
            for (String uri : locationHints) {
                // Cut off protocol declaration.
                if (uri.startsWith("file:")) {
                    uri = uri.substring(5);
                }
                
                try {
                    doc = database.getDocument(uri);
                    break;
                } catch (XmldbException e) {
                    // Invalid URI; try next location hint.
                }
            }
            
            StreamSource[] sources = null;
            
            if (doc != null) {
                try {
                    sources = new StreamSource[] {
                        new StreamSource(doc.getContent(), doc.getUri()),
                    };
                } catch (XmldbException e) {
                    logger.error(String.format(
                            "Error retrieving content of document '%s': %s",
                            doc, e.getMessage()));
                }
            }
            
            return sources;
        }
        
    } // XmldbModuleURIResolver

}
