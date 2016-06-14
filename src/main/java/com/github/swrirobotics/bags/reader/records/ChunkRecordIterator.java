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

import com.github.swrirobotics.bags.reader.BagFile;
import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import com.github.swrirobotics.bags.reader.records.ChunkInfo;
import com.github.swrirobotics.bags.reader.records.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Messages stored for a connection can by split across multiple chunks.
 * This class provides a way to iterate through all of the chunks
 * associated with a particular connection.
 */
public class ChunkRecordIterator implements Iterator<Record> {
    private final SeekableByteChannel myInput;
    private final Iterator<ChunkInfo> myChunkIter;
    private final int myConnId;
    private Record myNextRecord = null;

    private static final Logger myLogger = LoggerFactory.getLogger(ChunkRecordIterator.class);

    /**
     * Creates the iterator.
     * @param connectionId The ID of the connection to get chunks for.
     * @param input An open FileChannel to read chunks from; this should
     *              not be closed until after you are done using the iterator.
     * @param chunkInfos All of the chunk info records to search through for
     *                   connections.
     */
    public ChunkRecordIterator(int connectionId,
                               final SeekableByteChannel input,
                               final List<ChunkInfo> chunkInfos) {
        this.myConnId = connectionId;
        this.myInput = input;

        myChunkIter = chunkInfos.iterator();
    }

    @Override
    public boolean hasNext() {
        if (myNextRecord == null) {
            myNextRecord = findNext();
        }

        return myNextRecord != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Record tmp = myNextRecord;
        myNextRecord = null;

        return tmp;
    }

    private Record findNext() {
        if (!myChunkIter.hasNext()) {
            return null;
        }

        long chunkPos = -1;
        while (myChunkIter.hasNext() && chunkPos == -1) {
            // Iterate through our chunks until we find one that matches our connection ID.
            ChunkInfo info = myChunkIter.next();
            for (ChunkInfo.ChunkConnection conn : info.getConnections()) {
                if (conn.getConnectionId() == myConnId) {
                    chunkPos = info.getChunkPos();
                    break;
                }
            }
        }
        if (chunkPos == -1) {
            return null;
        }

        try {
            Record chunk = BagFile.recordAt(myInput, chunkPos);
            chunk.readData();
            return chunk;
        }
        catch (BagReaderException e) {
            myLogger.warn("Error reading data chunk", e);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
