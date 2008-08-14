package xantippe;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/**
 * <code>OutputStream</code> for writing the content of a document.
 * 
 * The content is only written to the database after calling the
 * <code>close</code> method.
 * 
 * @author Oscar Stigter
 */
public class InsertStream extends OutputStream {
    
    
    /** The document. */
    private final Document document;
    
    /** Temporary file with the document content. */
    private final File file;
    
    /** Underlying output stream to the temporary file. */
    private final OutputStream os;
    

    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------
    

    /**
     * Constructor.
     * 
     * @param  document  the associated document
     */
    /* protected */ InsertStream(Document document) throws IOException {
        this.document = document;
        file = File.createTempFile("xantippe-", null);
        os = new FileOutputStream(file);
    }
    
    
    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------
    

    /**
     * Closes the stream.
     * 
     * @throws IOException  if the stream could not be gracefully closed
     */
    @Override
    public void close() throws IOException {
        try {
            os.close();
            
            try {
                document.setContent(file);
            } catch (XmldbException e) {
                throw new IOException(e.getMessage());
            }
        } finally {
            file.delete();
        }
    }


    /**
     * Flushes the stream, writing any non-written data.
     * 
     * @throws IOException  if the data could not be written
     */
    @Override
    public void flush() throws IOException {
        os.flush();
    }


    /**
     * Writes a byte.
     * 
     * @param  b  the byte
     * 
     * @throws  IOException  if the byte could not be written
     */
    @Override
    public void write(int b) throws IOException {
        os.write(b);
    }


    /**
     * Writes a byte array.
     * 
     * @param  data  the byte array
     * 
     * @throws  IOException  if the data could not be written
     */
    @Override
    public void write(byte[] data) throws IOException {
        os.write(data);
    }


    /**
     * Writes a byte array.
     * 
     * @param  data    the byte array
     * @param  offset  the offset in the array to start from
     * @param  length  the number of bytes to write
     * 
     * @throws  IOException  if the data could not be written
     */
    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException {
        os.write(buffer, offset, length);
    }


}
