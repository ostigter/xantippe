package xantippe;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Indexer for XML documents.
 * 
 * Implemented with SAX for high performance.
 * 
 * @author Oscar Stigter
 */
public class Indexer {
    
    
    /** log4j logger. */
    private static final Logger logger = Logger.getLogger(Indexer.class);
    
    /** SAX parser. */
    private final SAXParser parser;
    
    /** Content handler to extract the index values. */
    private final XPathHandler handler = new XPathHandler(); 
    
    
    public Indexer() {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            parser = factory.newSAXParser();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error instantiating SAX parser: " + e.getMessage());
        }
    }
    
    
    public void index(Document doc, File file, Collection col) {
        if (col.getIndices(true).size() > 0) {
            logger.debug(String.format("Index document '%s'", doc));
            
            long startTime = System.currentTimeMillis();
            
            handler.setDocument(doc);
            handler.setCollection(col);
            
            try {
                parser.parse(file, handler);
            } catch (Exception e) {
                // Should never happen; document has already been validated.
                String msg = String.format(
                        "Error while indexing document '%s': %s",
                        doc, e.getMessage());
                logger.error(msg, e);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.debug(String.format("Indexed document in %d ms.", duration));
        }
    }
    
    
    //------------------------------------------------------------------------
    //  Inner classes
    //------------------------------------------------------------------------
    
    
    private class XPathHandler extends DefaultHandler {
        
        
        private final List<String> elements;
        
        private Collection collection;
        
        private Set<Index> indices;
        
        private Document document;
        
        private StringBuilder sb;
        
        
        public XPathHandler() {
            elements = new ArrayList<String>();
        }
        
        
        public void setCollection(Collection col) {
        	collection = col;
        	indices = col.getIndices(true);        	
        }
        
        public void setDocument(Document doc) {
        	document = doc;
        }
        
        
        @Override
        public void startElement(
                String uri, String localName, String qName, Attributes attr)
                throws SAXException {
            elements.add(localName);
            sb = new StringBuilder();
        }
        
        
        @Override
        public void endElement(String uri, String localName, String qName)
                throws SAXException {
            String path = getCurrentPath();
//            System.out.println("Current path: " + path);
            for (Index index : indices) {
//                System.out.println("Index path:   " + index.getPath());
            	if (path.equals(index.getPath()) ||
            			(index.getPath().startsWith("//") &&
            				path.endsWith(index.getPath().substring(1)))) {
            		String text = sb.toString().trim();
            		if (text.length() != 0) {
            			Object value = getIndexValue(text, index.getType());
            			if (value != null) {
            				collection.addIndexValue(
            						index.getName(), value, document.getId());
            			}
                    }
                }
            }
            elements.remove(elements.size() - 1);
            sb = new StringBuilder();
        }
        

        @Override
        public void characters(char[] buffer, int offset, int length)
                throws SAXException {
            sb.append(buffer, offset, length);
        }
        
        
        private String getCurrentPath() {
            StringBuilder sb2 = new StringBuilder();
            for (String element : elements) {
                sb2.append('/');
                sb2.append(element);
            }
            return sb2.toString();
        }
        
        
        private Object getIndexValue(String text, IndexType type) {
        	Object value = null;
        	
			switch (type) {
				case STRING:
					value = text;
					break;
				case INTEGER:
					try {
						value = Integer.parseInt(text);
					} catch (Exception e) {
						// Ignore.
					}
					break;
				case LONG:
					try {
						value = Long.parseLong(text);
					} catch (Exception e) {
						// Ignore.
					}
					break;
				case DOUBLE:
					try {
						value = Double.parseDouble(text);
					} catch (Exception e) {
						// Ignore.
					}
					break;
				case FLOAT:
					try {
						value = Float.parseFloat(text);
					} catch (Exception e) {
						// Ignore.
					}
					break;
				case DATE:
					//TODO: Parse date
					break;
				default:
					// Invalid index type; ignore.
			}
			
        	return value;
        }
        
        
    }


}
