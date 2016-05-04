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

package com.github.swrirobotics.bags.reader.messages.serialization;

import com.github.swrirobotics.bags.reader.*;
import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import com.github.swrirobotics.bags.reader.exceptions.UnknownMessageException;
import com.github.swrirobotics.bags.reader.records.ChunkInfo;
import com.github.swrirobotics.bags.reader.records.ChunkRecordIterator;
import com.github.swrirobotics.bags.reader.records.Connection;
import com.github.swrirobotics.bags.reader.records.Record;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This iterator will find all of the ROS messages sent over a list
 * of connection IDs and provide a mechanism to iterate over them.
 * Note that the messages are not guaranteed to be in any particular order.
 */
public class MsgIterator implements Iterator<MessageType> {
    final private SeekableByteChannel myInput;
    final private List<ChunkInfo> myChunkInfos;
    final private Iterator<Connection> myConnections;

    private Connection myCurrentConn = null;
    private ByteBufferChannel currentBuffer = null;
    private ChunkRecordIterator chunkIter = null;

    private MessageType myCurrentConnMt = null;
    private MessageType nextMsg = null;

    private static final Logger myLogger = LoggerFactory.getLogger(MsgIterator.class);

    /**
     * Creates a MsgIterator.
     * @param chunkInfos A collection of ChunkInfos for the bag file you're
     *                   reading from.
     * @param connId A connection ID to search through.
     * @param input An open FileChannel to read data chunks from.  This
     *              should not be closed until after you are done using
     *              the iterator.
     */
    public MsgIterator(final List<ChunkInfo> chunkInfos,
                       final Connection connId,
                       final SeekableByteChannel input) {
        this(chunkInfos, Lists.newArrayList(connId), input);
    }

    /**
     * Creates a MsgIterator.  Just like the other constructor except
     * this will search through multiple connections.
     * @param chunkInfos A collection of ChunkInfos for the bag file you're
     *                   reading from.
     * @param conns A list of connections to search through.
     * @param input An open FileChannel to read data chunks from.  This
     *              should not be closed until after you are done using
     *              the iterator.
     */
    public MsgIterator(final List<ChunkInfo> chunkInfos,
                       final List<Connection> conns,
                       final SeekableByteChannel input) {
        myConnections = conns.iterator();
        this.myInput = input;
        myChunkInfos = chunkInfos;
    }

    @Override
    public boolean hasNext() {
        // Because record chunks can be filled with bytes that aren't actually
        // message data and we don't know what they are until we examine them,
        // the only way to know if there is another message is to go ahead and
        // search for it.
        if (nextMsg == null) {
            nextMsg = findNext();
        }
        return nextMsg != null;
    }

    /**
     * Returns a MessageType object that has read and is ready to provide all of
     * the data for the next message in sequence.
     * IMPORTANT NOTE: This call can return a MessageType object that is technically the
     * same as a previous call.  Initializing new MessageType objects is very expensive,
     * and the MsgIterator will re-use old ones for performance when possible.  Do not
     * try to save and re-use previous MessageTypes!  Extract the info you need from each
     * one before you get the next one.
     * @return A MessageType that has deserialized the next message on the connection.
     */
    @Override
    public MessageType next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (nextMsg != null) {
            MessageType tmp = nextMsg;
            nextMsg = null;
            return tmp;
        }

        throw new NoSuchElementException();
    }

    private MessageType findNext() {
        // If we don't have an open ByteBuffer, get the one for the next connection.
        if (currentBuffer == null) {
            if (chunkIter != null && chunkIter.hasNext()) {
                // If we have an active chunk iterator, get the next chunk and load
                // it into the buffer.
                Record nextChunk = chunkIter.next();
                try {
                    currentBuffer = new ByteBufferChannel(nextChunk.getData());
                }
                catch (BagReaderException e) {
                    return null;
                }
            }
            else {
                // If we don't have an active chunk iterator, that means we need
                // to go to the next connection and load an iterator for it.
                if (!myConnections.hasNext()) {
                    // If there are no more connections remaining, there are no more elements.
                    return null;
                }

                myCurrentConn = myConnections.next();
                try {
                    myCurrentConnMt = myCurrentConn.getMessageCollection().getMessageType();
                }
                catch (UnknownMessageException e) {
                    myLogger.error("Unable to deserialize messages for connection " + myCurrentConn.getConnectionId());
                    return null;
                }

                chunkIter = new ChunkRecordIterator(myCurrentConn.getConnectionId(), myInput, myChunkInfos);
                // After we've loaded a new iterator, we can recurse down and try to
                // load from it...
                return findNext();
            }
        }

        try {
            // Find the next message in the buffer that is data and double-check
            // that it matches our connection ID.
            while (currentBuffer.position() < currentBuffer.size()) {
                Record record = new Record(currentBuffer);
                if (record.getHeader().getType() == Record.RecordType.MESSAGE_DATA &&
                        record.getHeader().getInt("conn") == myCurrentConn.getConnectionId()) {

                    ByteBuffer buf = record.getData().order(ByteOrder.LITTLE_ENDIAN);
                    myCurrentConnMt.reset();
                    myCurrentConnMt.readMessage(buf);
                    return myCurrentConnMt;
                }
            }
        }
        catch (BagReaderException | IOException | RuntimeException e) {
            myLogger.error("Error reading messages", e);
            return null;
        }

        // If we got through the above while loop, that means we didn't find any messages
        // in the current buffer.  Set it to null and try again; that will make it
        // either read the next buffer and return the first message out of it, or if
        // there aren't any more connections, it'll throw an exception.
        // TODO pjr Since this is recursive, a very long sequence of chunks that don't
        //          have any data messages could cause a stack overflow...
        currentBuffer = null;
        return findNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
