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

import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an index data record in a bag file.
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Index_data">http://wiki.ros.org/Bags/Format/2.0#Index_data</a>
 */
public class IndexData {
    private int myVersion;
    private int myConnectionId;
    private int myCount;
    private final List<Index> myIndexes = new ArrayList<>();
    private final Chunk myChunk;

    public IndexData(Record record, Chunk chunk) throws BagReaderException {
        myVersion = record.getHeader().getInt("ver");
        myConnectionId = record.getHeader().getInt("conn");
        myCount = record.getHeader().getInt("count");
        myChunk = chunk;

        ByteBuffer buffer = record.getData();
        for (int i = 0; i < myCount; i++) {
            long secs = (long) buffer.getInt();
            int nsecs = buffer.getInt();
            int offsetVal = buffer.getInt();
            myIndexes.add(new Index(secs, nsecs, offsetVal));
        }
    }

    public int getVersion() {
        return myVersion;
    }

    public int getConnectionId() {
        return myConnectionId;
    }

    public int getCount() {
        return myCount;
    }

    public Chunk getChunk() {
        return myChunk;
    }

    public List<Index> getIndexes() {
        return myIndexes;
    }

    /**
     * Represents an individual index within an index data record in a bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Index_data">http://wiki.ros.org/Bags/Format/2.0#Index_data</a>
     */
    public static class Index {
        private final Timestamp myTime;
        private final int myOffset;

        public Index(long secs, int nsecs, int offset) {
            this.myTime = new Timestamp(secs * 1000L);
            this.myTime.setNanos(nsecs);
            this.myOffset = offset;
        }

        public Timestamp getTime() {
            return myTime;
        }

        public int getOffset() {
            return myOffset;
        }
    }
}
