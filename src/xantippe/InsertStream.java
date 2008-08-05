package xantippe;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/**
 * <code>OutputStream</code> for writing the content of a document.
 * 
 * The content is actually written to the database upon calling the
 * <code>close</code> method.
 * 
 * @author Oscar Stigter
 */
public class InsertStream extends OutputStream {
	
	
	/** The document. */
	private final Document document;
	
	/** Temporary file. */
	private final File file;
	
	private final OutputStream os;
	

    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------
    

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
		os.close();
		
		try {
			document.setContent(file);
		} catch (XmldbException e) {
			throw new IOException(e);
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


	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}


	@Override
	public void write(byte[] buffer) throws IOException {
		os.write(buffer);
	}


	@Override
	public void write(byte[] buffer, int offset, int length)
			throws IOException {
		os.write(buffer, offset, length);
	}


    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------
    

	/**
	 * Returns the temporary file with the document contents.
	 * 
	 * @return  the file
	 */
	/* package */ File getFile() {
		return file;
	}


}
