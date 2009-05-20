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


package org.ozsoft.xantippe.filestore;


import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;


/**
 * InputStream used for reading the content of a stored document.
 * 
 * @author Oscar Stigter
 */
/* package */ class RetrieveStream extends InputStream {
    
    
    /** The data file. */
    private final RandomAccessFile dataFile;
    
    /** The file length in bytes. */
    private final int length;
    
    /** The current position in the stream. */ 
    private int position = 0;
    
    
    //------------------------------------------------------------------------
    //  Constructor
    //------------------------------------------------------------------------
    

    /* package */ RetrieveStream(
            RandomAccessFile dataFile, int offset, int length)
            throws IOException {
        this.dataFile = dataFile;
        this.length = length;
        dataFile.seek(offset);
    }


    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------
    
    
    /**
     * Returns the number of bytes that can still be read.
     * 
     * @return  the number of bytes that can still be read
     * 
     * @throws  IOException  never thrown
     */
    @Override
    public int available() throws IOException {
        return (length - position);
    }


    /**
     * Closes the stream.
     * 
     * @throws  IOException  if the stream could not be closed
     */
    @Override
    public void close() throws IOException {
        super.close();
    }


    /**
     * Marks the current position in the stream.
     * 
     * Not supported.
     * 
     * @throws  UnsupportedOperationException  always
     */
    @Override
    public synchronized void mark(int position) {
        throw new UnsupportedOperationException();
    }


    /**
     * Returns true if this class supports the mark() method, otherwise false.
     * 
     * Since mark() is not implemented, this method will always return false.
     * 
     * @return false (fixed)
     */
    @Override
    public boolean markSupported() {
        // Not implemented.
        return false;
    }


    /**
     * Returns the next byte as integer in the range [0, 255], or -1 if no more
     * bytes are available.
     * 
     * @return  the next byte, or -1 if not available
     *  
     * @throws  IOException  if the byte could not be read
     */
    @Override
    public int read() throws IOException {
        int value;
        
        if (available() > 0) {
            value = dataFile.read();
            position++;
        } else {
            value = -1;
        }
        
        return value;
    }


    /**
     * Reads as many bytes as necessary to fill the specified buffer.
     * 
     * @param  buffer  the buffer
     * 
     * @return  the number of bytes actually read
     *  
     * @throws  IllegalArgumentException  if the buffer is null
     * @throws  IOException               if the stream could not be read
     */
    @Override
    public int read(byte[] buffer) throws IOException {
        if (buffer == null) {
            throw new IllegalArgumentException("Null buffer");
        }
        
        return read(buffer, 0, buffer.length);
    }


    /**
     * Reads the specified number of bytes and fills the specified buffer.
     * 
     * @param   buffer  the buffer
     * @param   offset  the buffer offset
     * @param   length  the number of bytes to read
     * 
     * @return  the actual number of bytes read, or -1 if no more bytes are
     *          available
     *  
     * @throws  IllegalArgumentException
     *              if the buffer is null, the offset is invalid, the length is
     *              invalid
     * @throws  IOException  if the stream could not be read
     */
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
        if (offset >= buffer.length) {
            throw new IllegalArgumentException("Invalid offset (too large)");
        }
        if ((offset + length) > buffer.length) {
            throw new IllegalArgumentException("Buffer overflow");
        }

        if (length > available()) {
            length = available();
        }
        
        int read;
        if (length > 0) {
            read = dataFile.read(buffer, offset, length);
            position += read;
        } else {
            read = -1;
        }
        
        return read;
    }


    /**
     * Resets the stream to the last mark() call.
     * 
     * Not implemented.
     *  
     * @throws  IOException                    never
     * @throws  UnsupportedOperationException  always
     */
    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException();
    }


    /**
     * Skips the specified number of bytes.
     * 
     * The bytes are skipped by reading and immediately discarding them.
     * 
     * @param   length  the number of bytes to skip
     * 
     * @return  the number of bytes actually skipped
     *  
     * @throws  IOException  in no bytes could be skipped due to an error
     */
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
