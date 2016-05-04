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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a chunk info record in a bag file.
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk_info">http://wiki.ros.org/Bags/Format/2.0#Chunk_info</a>
 */
public class ChunkInfo {
    private int myVersion;
    private long myChunkPos;
    private Timestamp myStartTime;
    private Timestamp myEndTime;
    private int myCount;
    private final List<ChunkConnection> myConnections = new ArrayList<>();

    public ChunkInfo(Record record) throws BagReaderException {
        myVersion = record.getHeader().getInt("ver");
        myChunkPos = record.getHeader().getLong("chunk_pos");
        myStartTime = record.getHeader().getTimestamp("start_time");
        myEndTime = record.getHeader().getTimestamp("end_time");
        myCount = record.getHeader().getInt("count");

        int[] connectionCounts = new int[myCount * 2];

        record.getData().asIntBuffer().get(connectionCounts);
        for (int i = 0; i < connectionCounts.length; i += 2) {
            myConnections.add(new ChunkConnection(
                    connectionCounts[i], connectionCounts[i+1]));
        }
    }

    public int getVersion() {
        return myVersion;
    }

    public long getChunkPos() {
        return myChunkPos;
    }

    public Timestamp getStartTime() {
        return myStartTime;
    }

    public Timestamp getEndTime() {
        return myEndTime;
    }

    public int getCount() {
        return myCount;
    }

    public List<ChunkConnection> getConnections() {
        return myConnections;
    }

    /**
     * Represents a connection inside a chunk info block in a bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk_info">http://wiki.ros.org/Bags/Format/2.0#Chunk_info</a>
     */
    public static class ChunkConnection {
        private final int myConnectionId;
        private final int myMessageCount;

        public ChunkConnection(int conn, int count) {
            myConnectionId = conn;
            myMessageCount = count;
        }

        public int getConnectionId() {
            return myConnectionId;
        }

        public int getMessageCount() {
            return myMessageCount;
        }
    }
}
