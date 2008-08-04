package xantippe.filestore;


import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * <code>InputStream</code> used for reading the contents of a stored file.
 * 
 * @author Oscar Stigter
 */
public class RetrieveStream extends InputStream {
    
    
    private final RandomAccessFile file;
    
    private final int length;
    
    private int position = 0;
    
    
    /* package */ RetrieveStream(
            RandomAccessFile file, int offset, int length) throws IOException {
        this.file = file;
        this.length = length;
        file.seek(offset);
    }


    @Override
    public int available() throws IOException {
        return (length - position);
    }


    @Override
    public void close() throws IOException {
        super.close();
    }


    @Override
    public synchronized void mark(int position) {
        throw new NotImplementedException();
    }


    @Override
    public boolean markSupported() {
        // Not implemented.
        return false;
    }


    @Override
    public int read() throws IOException {
        int value;
        
        if (available() > 0) {
            value = file.read();
            position++;
        } else {
            value = -1;
        }
        
        return value;
    }


    @Override
    public int read(byte[] buffer) throws IOException {
        if (buffer == null) {
            throw new IllegalArgumentException("Null buffer");
        }
        
        return read(buffer, 0, buffer.length);
    }


    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (buffer == null) {
            throw new IllegalArgumentException("Null buffer");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid offset (negative)");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Invalid length (negative)");
        }
        if (offset > buffer.length) {
            throw new IllegalArgumentException("Invalid offset (too large)");
        }
        if ((offset + length) > buffer.length) {
            throw new IllegalArgumentException("Buffer overflow");
        }

        if (length > available()) {
            length = available();
        }
        int read = file.read(buffer, offset, length);
        position += read;
        return read;
    }


    @Override
    public synchronized void reset() throws IOException {
        throw new NotImplementedException();
    }


    @Override
    public long skip(long length) throws IOException {
        if (length > available()) {
            length = available();
        }

        for (long i = 0L; i < length; i++) {
            read();
        }

        return length;
    }


}
