// *****************************************************************************
//
// Copyright (c) 2015, Southwest Research Institute® (SwRI®)
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Southwest Research Institute® (SwRI®) nor the
//       names of its contributors may be used to endorse or promote products
//       derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL Southwest Research Institute® BE LIABLE 
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.
//
// *****************************************************************************

package com.github.swrirobotics.bags.reader.records;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

/**
 * Represents a generic record in a bag file.
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Records">http://wiki.ros.org/Bags/Format/2.0#Records</a>
 */
public class Record {
    private int headerLength;
    private Header myHeader;

    private Header myConnectionHeader = null;

    private long myDataOffset = -1;
    private int myDataLength;
    private ByteBuffer myData = null;
    private final SeekableByteChannel myChannel;

    private static final Logger myLogger = LoggerFactory.getLogger(Record.class);

    public enum RecordType {
        BAG_HEADER,
        CHUNK,
        CONNECTION,
        MESSAGE_DATA,
        INDEX_DATA,
        CHUNK_INFO,
        UNKNOWN
    }

    /**
     * Creates a new record from the given channel.
     * This method assumes that the channel is already open and its current
     * position is the beginning of the record.  Note that if this record
     * has a data field, for the sake of efficiency it does NOT automatically
     * read that field; you must manually call {@link #getData()} or
     * {@link #readData()} in order for it to do so.
     * Additionally, this class does not automatically close the byte channel;
     * you must do so after you are done with it.  After the channel has been
     * closed, do not call {@link #readData()}, and do not call {@link #getData()}
     * unless you had previously done so.
     * @param channel A byte channel that points to a record.
     * @throws BagReaderException
     */
    public Record(SeekableByteChannel channel) throws BagReaderException {
        myChannel = channel;
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int count;
        try {
            count = myChannel.read(lengthBuffer);
            if (count != 4) {
                throw new BagReaderException("Unable to read header length; only got " +
                                                     count + " bytes.");
            }
            lengthBuffer.flip();
            this.headerLength = lengthBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();

            if (this.headerLength > 100000L) {
                throw new BagReaderException("Header is unreasonably large (" + this.headerLength +
                                                     " bytes).  Bag file may need to be reindexed.");
            }
            else if (this.headerLength == 0) {
                myLogger.warn("Saw a zero-byte header.  Skipping.");
                myChannel.position(myChannel.position() + 4); // Skip the next four bytes, too
                this.myHeader = new Header();
                return;
            }
            else {
                ByteBuffer headerBuffer = ByteBuffer.allocate(this.headerLength);
                count = myChannel.read(headerBuffer);
                if (count != this.headerLength) {
                    throw new BagReaderException("Unable to read header (size " +
                                                         this.headerLength + "); only got " + count + " bytes.");
                }

                headerBuffer.flip();
                this.myHeader = new Header(headerBuffer);
            }

            lengthBuffer.flip();
            count = myChannel.read(lengthBuffer);
            if (count != 4) {
                throw new BagReaderException("Unable to read data length; only got " +
                                                     count + " bytes.");
            }
            lengthBuffer.flip();
            this.myDataLength = lengthBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();

            this.myDataOffset = myChannel.position();
            channel.position(channel.position() + this.myDataLength);
        }
        catch (NegativeArraySizeException e) {
            throw new BagReaderException("Unable to read header (size " + this.headerLength + ")");
        }
        catch (IOException e) {
            throw new BagReaderException("Unable to read header.");
        }
    }

    /**
     * Reads the data field of the record.  If the record is compressed, this
     * will involve decompressing it.  Currently both bz2 and lz4 compression
     * are supported.
     * Do not call this method more than once, and do not call it if the channel
     * used to create this record has been closed.
     * @throws BagReaderException
     */
    public void readData() throws BagReaderException {
        myData = ByteBuffer.allocate(myDataLength);
        try {
            myChannel.position(myDataOffset);
            myChannel.read(myData);

            // Chunks can have bz2 or lz4-compressed myData in them, which we need to
            // decompress in order to do anything useful with.
            if (myHeader.getType() == RecordType.CHUNK) {
                String compression = myHeader.getValue("compression");
                switch (compression) {
                    case "none":
                        // Do nothing here if not compressed
                        break;
                    case "bz2":
                    case "lz4":
                    {
                        int decompressedSize = myHeader.getInt("size");
                        myData.flip();
                        try (ByteBufferBackedInputStream inStream = new ByteBufferBackedInputStream(myData);
                             InputStream compressedStream = openCompressedStream(compression, inStream)) {
                            final byte[] buffer = new byte[decompressedSize];
                            int n = IOUtils.readFully(compressedStream, buffer);
                            if (n != decompressedSize) {
                                throw new BagReaderException("Read " + n + " bytes from a " +
                                                             "compressed chunk but expected " +
                                                             decompressedSize + ".");
                            }

                            myData = ByteBuffer.wrap(buffer);
                        }
                        break;
                    }
                    default:
                        myLogger.warn("Unknown compression format: " + compression);
                        break;
                }
            }
            myData.order(ByteOrder.LITTLE_ENDIAN);
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }
    }

    /**
     * Opens an input stream that will read compressed data of the specified type
     * from a compressed input stream.
     * @param compressionType The type of compression; currently supported values
     *                        are "bz2" and "lz4".
     * @param inStream An InputStream containing compressed data.
     * @return An InputStream that will decompress data from that stream.
     * @throws IOException If there was an error opening the stream.
     * @throws BagReaderException If the compression type is not supported.
     */
    private InputStream openCompressedStream(String compressionType, InputStream inStream)
            throws IOException, BagReaderException {
        switch (compressionType) {
            case "bz2":
                return new BZip2CompressorInputStream(inStream);
            case "lz4":
                return new LZ4FrameInputStream(inStream);
            default:
                String error = "Unknown compression type: " + compressionType;
                throw new BagReaderException(error);
        }
    }

    /**
     * If this record represents a connection and {@link #setConnectionHeader(Header)}
     * was previously called, returns this connection's header.
     * @return The connection header, if one was set; otherwise null.
     */
    public Header getConnectionHeader() {
        return myConnectionHeader;
    }

    /**
     * If this record represents a connection, this is used to set the connection
     * header inside.
     * @param connectionHeader This record's connection header.
     */
    public void setConnectionHeader(Header connectionHeader) {
        this.myConnectionHeader = connectionHeader;
    }

    /**
     * Returns the record's header.
     * @return The record's header.
     */
    public Header getHeader() {
        return myHeader;
    }

    /**
     * Returns a byte buffer containing the data inside the record.  The buffer's
     * position will be set to 0.
     * This method must be called for the first time before the channel the
     * record was created with is closed.  After the channel is closed, this
     * method can be called multiple times, and it will return the same byte
     * buffer every time.
     * @return A byte buffer containing this record's data.
     * @throws BagReaderException
     */
    public ByteBuffer getData() throws BagReaderException {
        if (myData == null) {
            readData();
        }
        myData.position(0);
        return myData;
    }
}
