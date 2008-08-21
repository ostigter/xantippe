package xantippe;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.log4j.Logger;
import org.w3c.dom.ls.LSInput;
import org.w3c.dom.ls.LSResourceResolver;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Document validator.
 * 
 * @author Oscar Stigter
 */
public class DocumentValidator {
    

    /** Database file with the schema's. */
    private static final String SCHEMAS_FILE = "schemas.dbx";
    
    /** log4j logger. */
    private static final Logger logger =
            Logger.getLogger(DocumentValidator.class);
    
    /** Schema factory for WC3 XML schema's with namespaces. */
    private final SchemaFactory schemaFactory =
            SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    /** Schema files mapped by the schema namespace. */
    private final Map<String, Integer> schemaFiles; 
    
    /** Cached Validator objects mapped by their schema namespace. */
    private final Map<String, Validator> validators;
    
    /** SAX parser. */
    private final SAXParser parser;
    
    /** Back-reference to the database. */
    private final DatabaseImpl database;

    //------------------------------------------------------------------------
    //  Constructors.
    //------------------------------------------------------------------------


    /**
     * Constructor.
     */
    /* package */ DocumentValidator(DatabaseImpl database) {
        this.database = database;
        
        schemaFactory.setResourceResolver(new XmldbResourceResolver());
        
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        try {
            parser = spf.newSAXParser();
        } catch (Exception e) {
            String msg = "Could not instantiate SAX parser";
            logger.fatal(msg, e);
            throw new RuntimeException(msg, e);
        }
        
        schemaFiles = new HashMap<String, Integer>();
        validators = new HashMap<String, Validator>();
    }
    
    
    //------------------------------------------------------------------------
    //  Public methods.
    //------------------------------------------------------------------------


    /**
     * Clears all schema namespaces and cached Validator objects.
     */
    public void clearSchemas() {
        schemaFiles.clear();
        validators.clear();
        logger.debug("Cleared all schema's");
    }
    
    
    /**
     * Adds a schema.
     * 
     * @param  file   the schema file
     * @param  docId  the document ID 
     */
    public void addSchema(File file, int docId)
            throws IOException, SAXException, XmldbException {
        String namespace = getTargetNamespaceFromSchema(file);
        schemaFiles.put(namespace, docId);
        logger.debug(String.format(
                "Added schema with namespace '%s'", namespace));
        //TODO: Validate schema before storing it.
//      try {
//          // Validate schema by compiling it, but do not keep in cache.
//          getValidator(namespace, false);
//      } catch (XmldbException e) {
//          // Invalid schema; remove from table.
//          schemaFiles.remove(namespace);
//          throw e;
//      }
    }
    
    
    /**
     * Validates a document.
     * 
     * @param  file      the document
     * @param  required  whether validation is required
     * 
     * @return  true if the document is valid, otherwise false
     */
    public void validate(File file, boolean required) throws XmldbException {
        long startTime = System.currentTimeMillis();
        
        String namespace;
        try {
            namespace = getDocumentNamespace(file);
        } catch (IOException e) {
            String msg = String.format(
                    "Error reading file: '%s': ", file, e.getMessage());
            logger.error(msg);
            throw new XmldbException(msg);
        } catch (SAXException e) {
            String msg = String.format(
                    "Invalid document: '%s': ", file, e.getMessage());
            logger.warn(msg);
            throw new XmldbException(msg);
        }
            
        if (namespace != null && namespace.length() != 0) {
            Validator validator = getValidator(namespace, true);
            if (validator != null) {
                try {
                    validator.validate(new StreamSource(file));
                    long duration = System.currentTimeMillis() - startTime;
                    logger.debug(String.format(
                        "Document '%s' validated in %d ms", file, duration));
                } catch (Exception e) {
                    String msg = String.format(
                        "Invalid document: '%s': %s", file, e.getMessage());
                    logger.warn(msg);
                    throw new XmldbException(msg);
                }
            } else {
                if (required) {
                    String msg = String.format(
                            "Invalid document: '%s'; "
                            + "no schema found for namespace '%s'",
                            file, namespace);
                    logger.warn(msg);
                    throw new XmldbException(msg);
                }
            }
        } else {
            if (required) {
                String msg = String.format(
                        "Invalid document: '%s'; no document namespace", file);
                logger.warn(msg);
                throw new XmldbException(msg);
            }
        }
    }
    
    
    public void readSchemas(String dataDir) {
        clearSchemas();
        logger.debug(String.format("Read database file '%s'", SCHEMAS_FILE));
        File file = new File(dataDir + '/' + SCHEMAS_FILE);
        if (file.exists()) {
            try {
                DataInputStream dis =
                        new DataInputStream(new FileInputStream(file));
                int noOfSchemas = dis.readInt();
                for (int i = 0; i < noOfSchemas; i++) {
                    String namespace = dis.readUTF();
                    int docId = dis.readInt();
                    schemaFiles.put(namespace, docId);
                }
                dis.close();
            } catch (IOException e) {
                String msg = String.format(
                        "Error reading database file '%s': ",
                        SCHEMAS_FILE, e.getMessage());
                logger.error(msg);
            }
        }
    }
    
    
    public void writeSchemas(String dataDir) {
        logger.debug(String.format("Write database file '%s'", SCHEMAS_FILE));
        File file = new File(dataDir + '/' + SCHEMAS_FILE);
        try {
            DataOutputStream dos = new DataOutputStream(
                    new FileOutputStream(file));
            dos.writeInt(schemaFiles.size());
            for (String namespace : schemaFiles.keySet()) {
                int docId = schemaFiles.get(namespace);
                dos.writeUTF(namespace);
                dos.writeInt(docId);
            }
            dos.close();
        } catch (IOException e) {
            String msg = String.format(
                    "Error writing database file '%s': ",
                    SCHEMAS_FILE, e.getMessage());
            logger.error(msg);
        }
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods.
    //------------------------------------------------------------------------


    private Validator getValidator(String namespace, boolean keepInCache)
            throws XmldbException {
        logger.debug("Get schema for namespace '" + namespace + "'");
        
        // Try the cache first.
        Validator validator = validators.get(namespace);
        if (validator == null) {
            // Find matching schema file.
            Integer docId =  schemaFiles.get(namespace);
            if (docId != null) {
                Document doc = database.getDocument(docId);
                if (doc != null) {
                    // Compile schema.
                    long startTime = System.currentTimeMillis();
                    try {
                        Schema schema = schemaFactory.newSchema(
                                new StreamSource(doc.getContent()));
                        validator = schema.newValidator();
                        long duration = System.currentTimeMillis() - startTime;
                        logger.debug(String.format(
                                "Schema compiled in %d ms", duration));
                        if (keepInCache) {
                            // Store compiled schema in cache. 
                            validators.put(namespace, validator);
                        }
                    } catch (SAXException e) {
                        String msg = String.format(
                                "Error parsing schema file '%s': %s",
                                doc, e.getMessage());
                        logger.error(msg);
                        throw new XmldbException(msg);
                    }
                } else {
                    String msg = "Stored schema file no longer found";
                    logger.error(msg);
                    throw new XmldbException(msg);
                }
            } else {
                logger.debug("No matching schema file found (ignored)");
            }
        } else {
            logger.debug("Found precompiled schema");
        }
        
        return validator;
    }
    
    
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


    //------------------------------------------------------------------------
    
    
    /**
     * Resource resolver to retrieve schema files by their namespace.
     */
    private class XmldbResourceResolver implements LSResourceResolver {
        
        
        private static final String SCHEMA_TYPE =
                "http://www.w3.org/2001/XMLSchema";
        
    
        public LSInput resolveResource(String type, String namespace,
                String publicId, String systemId, String baseUri) {
            LSInput input = null;
            
            if (type.equals(SCHEMA_TYPE)) {
                logger.debug(String.format(
                        "Retrieve schema with namespace '%s'", namespace));
                Integer docId = schemaFiles.get(namespace);
                if (docId != null) {
                    Document doc = database.getDocument(docId);
                    if (doc != null) {
                        try {
                            InputStream is = doc.getContent();
                            if (is != null) {
                                input = new XmldbResourceInput();
                                input.setByteStream(is);
                            }
                        } catch (XmldbException e) {
                            String msg = "Error retrieving schema file: "
                                    + e.getMessage();
                            logger.error(msg);
                        }
                    } else {
                        String msg = String.format(
                                "Schema with namespace '%s' not found",
                                namespace);
                        logger.error(msg);
                    }
                } else {
                    String msg = String.format(
                            "Unknown schema with namespace '%s'", namespace);
                    logger.error(msg);
                }
            } else {
                String msg =
                        String.format("Unexpected resource type: '%s'", type);
                logger.error(msg);
            }
            
            return input;
        }
        
        
    }
    
    
    //------------------------------------------------------------------------
    
    
    /**
     * Resource input to retrieve schema's by their namespace from the
     * database as byte stream.  
     */
    private class XmldbResourceInput implements LSInput {
        
        
        private InputStream is;
        
        
        public String getPublicId() {
            // Empty implementation.
            return null;
        }


        public void setPublicId(String publicId) {
            // Empty implementation.
        }


        public String getSystemId() {
            // Empty implementation.
            return null;
        }

        
        public void setSystemId(String systemId) {
            // Empty implementation.
        }
        

        public String getBaseURI() {
            // Empty implementation.
            return null;
        }


        public void setBaseURI(String baseUri) {
            // Empty implementation.
        }


        public InputStream getByteStream() {
            return is;
        }


        public void setByteStream(InputStream is) {
            this.is = is;
        }


        public boolean getCertifiedText() {
            // Not supported.
            return false;
        }

        
        public void setCertifiedText(boolean isCertifiedText) {
            // Empty implementation.
        }


        public Reader getCharacterStream() {
            // Empty implementation.
            return null;
        }

        
        public void setCharacterStream(Reader reader) {
            // Empty implementation.
        }


        public String getEncoding() {
            // Empty implementation.
            return null;
        }


        public void setEncoding(String encoding) {
            // Empty implementation.
        }


        public String getStringData() {
            // Empty implementation.
            return null;
        }

        
        public void setStringData(String stringData) {
            // Empty implementation.
        }


    }
        

}
