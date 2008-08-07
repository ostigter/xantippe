package xantippe;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Document validator.
 * 
 * @author Oscar Stigter
 */
public class DocumentValidator {
	
	
    /** Schema factory for WC3 XML schema's with namespaces. */
    private static final SchemaFactory schemaFactory =
            SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    /** SAX parser. */
    private final SAXParser parser;

	/** Cached Validator objects mapped by their schema namespace. */
	private final Map<String, Validator> validators; 
	
	
	/**
	 * Constructor.
	 */
	/* package */ DocumentValidator() {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        try {
            parser = spf.newSAXParser();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate SAX parser: " + e);
        }
        
		validators = new HashMap<String, Validator>();
	}
	
	
	/**
	 * Adds a schema.
	 * 
	 * @param  file  the schema file
	 */
	public void addSchema(File file) throws IOException, SAXException {
		String namespace = getTargetNamespaceFromSchema(file);
		Schema schema = schemaFactory.newSchema(file);
		Validator validator = schema.newValidator();
		validators.put(namespace, validator);
	}
	
	
	/**
	 * Clears all schema's.
	 */
	public void clear() {
		validators.clear();
	}
	
	
	/**
	 * Validates a document.
	 * 
	 * @param  file      the document
	 * @param  required  whether validation is required
	 * 
	 * @return  true if the document is valid, otherwise false
	 */
	public boolean validateDocument(File file, boolean required) {
		boolean isValid = false;
		
		String namespace = null;
		try {
			namespace = getDocumentNamespace(file);
			if (namespace != null) {
				Validator validator = validators.get(namespace);
				if (validator != null) {
					try {
						validator.validate(new StreamSource(file));
						isValid = true;
					} catch (Exception e) {
						System.err.println(e);
					}
				} else {
					//TODO: Unknown schema.
				}
			} else {
				isValid = !required;
			}
		} catch (Exception e) {
			//TODO: Error parsing document.
		}
		
		return isValid;
	}
	
	
    //------------------------------------------------------------------------
    //  Private methods.
    //------------------------------------------------------------------------


    /**
     * Returns the target namespace from a schema file.
     *
     * @param   file  the schema file
     *
     * @return  the target namespace
     */
    private String getTargetNamespaceFromSchema(File file)
    		throws SAXException, IOException {
        String namespace = null;

        SchemaHandler handler = new SchemaHandler();
        try {
            parser.parse(file, handler);
        } catch (ParsingDoneException e) {
            // We have found what we were looking for.
            namespace = handler.getNamespace();
        }

        return namespace;
    }


    /**
     * Returns the document namespace from an XML file.
     *
     * @param   file  the XML file
     *
     * @return  the document namespace
     */
    private String getDocumentNamespace(File file)
    		throws SAXException, IOException {
        String namespace = null;

        DocumentHandler handler = new DocumentHandler();
        try {
            parser.parse(file, handler);
        } catch (ParsingDoneException e) {
            // We have found what we were looking for.
            namespace = handler.getNamespace();
        }

        return namespace;
    }


    //------------------------------------------------------------------------
    //  Inner classes.
    //------------------------------------------------------------------------


    /**
     * SAX content handler to retrieve the target namespace from a schema file.
     */
    private class SchemaHandler extends DefaultHandler {


    	/** Target namespace found. */
        private String namespace = null;


        /**
         * Returns the target namespace.
         *
         * @return  the target namespace
         */
        public String getNamespace() {
            return namespace;
        }


        /**
         * Processes the start of an XML element.
         *
         * @param  uri        the element's namespace URI
         * @param  localName  the element's local name
         * @param  qName      the element's qualified name
         * @param  attr       the element's attributes
         */
        @Override // DefaultHandler
        public void startElement(String uri, String localName, String qName,
                Attributes attr) throws SAXException {
            if (localName.equals("schema")) {
                namespace = attr.getValue("targetNamespace");
                throw new ParsingDoneException();
            }
        }
        
    
    }
    
    
    //------------------------------------------------------------------------
    
    
    /**
     * SAX content handler to retrieve the document namespace. 
     */
    private class DocumentHandler extends DefaultHandler {
        

    	/** Target namespace found. */
        private String namespace = null;
        
        
        /**
         * Returns the target namespace.
         * 
         * @return  the target namespace
         */
        public String getNamespace() {
            return namespace;
        }
        

        /**
         * Returns the root element namespace of an XML file, or null if it has
         * no namespace.
         *  
         * @param  file  the XML file
         * 
         * @return  the root element namespace, or null if no namespace
         */
        @Override // DefaultHandler
        public void startElement(String uri, String localName, String qName,
                Attributes attributes) throws SAXException {
            namespace = uri;
            throw new ParsingDoneException();
        }
        
    
    }
    
    
    //------------------------------------------------------------------------
    
    
    /**
     * Exception used to abort a SAX parser when all required data has been
     * collected.
     *
     * This trick boost performance when parsing large documents, especially
     * when the interesting data is located at the start of the document.
     */
    private class ParsingDoneException extends SAXException {
    	

        /** Serial version UID. */
        private static final long serialVersionUID = -1L;
        

    }


}
