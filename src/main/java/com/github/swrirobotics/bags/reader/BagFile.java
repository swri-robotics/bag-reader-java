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

package com.github.swrirobotics.bags.reader;

import com.github.swrirobotics.bags.reader.exceptions.BagReaderException;
import com.github.swrirobotics.bags.reader.exceptions.UnknownMessageException;
import com.github.swrirobotics.bags.reader.messages.serialization.MessageType;
import com.github.swrirobotics.bags.reader.messages.serialization.MsgIterator;
import com.github.swrirobotics.bags.reader.records.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.ProgressMonitor;

/**
 * Represents a bag file.  This class contains methods for reading a bag file
 * and accessing all of the different types of records that can be stored in one.
 */
public class BagFile {
    private final Path myPath;
    // We only support version 2.0 bag files at the moment.
    private static final String version = "2.0";
    private double myDurationS = 0.0;
    private Timestamp myStartTime = null;
    private Timestamp myEndTime = null;

    private BagHeader myBagHeader = null;
    private final List<Chunk> myChunks = Lists.newArrayList();
    private final List<Connection> myConnections = Lists.newArrayList();
    private final Map<Integer, Connection> myConnectionsById = Maps.newHashMap();
    private final Multimap<String, Connection> myConnectionsByTopic = HashMultimap.create();
    private final Multimap<String, Connection> myConnectionsByType = HashMultimap.create();
    private final Multimap<Integer, ChunkInfo> myChunkInfosByConnectionId = HashMultimap.create();
    private final List<MessageData> myMessages = Lists.newArrayList();
    private final List<IndexData> myIndexes = Lists.newArrayList();
    private final List<ChunkInfo> myChunkInfos = Lists.newArrayList();
    private final Map<String, List<MessageIndex>> myMessageIndexesForTopics = Maps.newHashMap();
    private Chunk myPreviousChunk = null;

    private static final Logger myLogger = LoggerFactory.getLogger(BagFile.class);

    /**
     * An index entry for one message, which includes topic and Timestamp if available
     */
    public static class MessageIndex implements Comparable<MessageIndex>, Serializable {
        private static final long serialVersionUID = 0L;
        public long fileIndex;
        public long chunkIndex;
        public String topic;
        public Timestamp timestamp;

        /**
         * Constructs a new MessageIndex
         *
         * @param fileIndex  the index in the file
         * @param chunkIndex the index of the chunk
         * @param topic      the topic of the message (needed for reading it)
         * @param timestamp  the timestamp, if it available, used for sorting the index
         */
        public MessageIndex(long fileIndex, long chunkIndex, String topic, Timestamp timestamp) {
            this.fileIndex = fileIndex;
            this.chunkIndex = chunkIndex;
            this.topic = topic;
            this.timestamp = timestamp;
        }

        /**
         * Returns comparison of Timestamp if it is not null, otherwise by fileIndex, then chunkIndex
         *
         * @param o the MessageIndex to compare to
         * @return result
         */
        @Override
        public int compareTo(MessageIndex o) {
            if (timestamp == null || o.timestamp == null) {
                int result = Long.compare(fileIndex, o.fileIndex);
                if (result != 0) {
                    return result;
                }
                else {
                    return Long.compare(chunkIndex, o.chunkIndex);
                }
            }
            else {
                return timestamp.compareTo(o.timestamp);
            }
        }

        @Override
        public String toString() {
            return "MessageIndex{" + "fileIndex=" + fileIndex + ", chunkIndex=" + chunkIndex + ", topic=" + topic + ", timestamp=" + timestamp + '}';
        }

    }

    /**
     * Constructs a new BagFile that represents a bag at the given path.
     * If you create a bag file using this constructor, you should then call
     * {@link #read()} to read the bag's data into memory.
     *
     * @param filePath The path of the file to open.
     */
    public BagFile(String filePath) {
        this.myPath = FileSystems.getDefault().getPath(filePath);
    }

    /**
     * Opens a read-only SeekableByteChannel that refers to the underlying bag file.
     *
     * @return An open SeekableByteChannel.
     * @throws IOException If there is an error opening the file.
     */
    public FileChannel getChannel() throws IOException {
        return FileChannel.open(getPath(), StandardOpenOption.READ);
    }

    /**
     * Gets the path to the bag file.  This will be the same as the string that
     * was passed to {@link #BagFile(String)}.
     *
     * @return The path to the bag file.
     */
    public Path getPath() {
        return myPath;
    }

    /**
     * The version of the bag file.  Currently, this will always be 2.0.
     *
     * @return 2.0
     */
    public String getVersion() {
        return version;
    }

    /**
     * The amount of time in seconds that the bag file spans.  If the start
     * time and end time could be read, this will be the difference between
     * them; otherwise it will be 0.0.
     *
     * @return The amount of time in seconds that the bag file spans.
     */
    public double getDurationS() {
        return myDurationS;
    }

    /**
     * The earliest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a start time.
     *
     * @return The bag's start time.
     */
    public Timestamp getStartTime() {
        return myStartTime;
    }

    /**
     * The latest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a end time.
     *
     * @return The bag's end time.
     */
    public Timestamp getEndTime() {
        return myEndTime;
    }

    /**
     * The bag's header record.
     *
     * @return The bag's header.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Bag_header">http://wiki.ros.org/Bags/Format/2.0#Bag_header</a>
     */
    public BagHeader getBagHeader() {
        return myBagHeader;
    }

    /**
     * All of the chunks in the bag file.
     *
     * @return All of the chunks in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk">http://wiki.ros.org/Bags/Format/2.0#Chunk</a>
     */
    public List<Chunk> getChunks() {
        return myChunks;
    }

    /**
     * All of the connections in the bag file.
     *
     * @return All of the connections in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Connection">http://wiki.ros.org/Bags/Format/2.0#Connection</a>
     */
    public List<Connection> getConnections() {
        return myConnections;
    }

    /**
     * Convenience method for adding a new connection; updates both the internal
     * list and id map.
     *
     * @param connection The connection to add.
     */
    private void addConnection(Connection connection) {
        myConnections.add(connection);
        myConnectionsById.put(connection.getConnectionId(), connection);
        myConnectionsByTopic.put(connection.getTopic(), connection);
        myConnectionsByType.put(connection.getType(), connection);
    }

    private void addChunkInfo(ChunkInfo chunkInfo) {
        myChunkInfos.add(chunkInfo);
        for (ChunkInfo.ChunkConnection conn : chunkInfo.getConnections()) {
            myChunkInfosByConnectionId.put(conn.getConnectionId(), chunkInfo);
        }
    }

    /**
     * All of the messages in the bag file.
     *
     * @return All of the messages in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Message_data">http://wiki.ros.org/Bags/Format/2.0#Message_data</a>
     */
    public List<MessageData> getMessages() {
        return myMessages;
    }

    /**
     * All of the indexes in the bag file.
     *
     * @return All of the indexes in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Index_data">http://wiki.ros.org/Bags/Format/2.0#Index_data</a>
     */
    public List<IndexData> getIndexes() {
        return myIndexes;
    }

    /**
     * All of the chunk infos in the bag file.
     *
     * @return All of the chunk infos in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk_info">http://wiki.ros.org/Bags/Format/2.0#Chunk_info</a>
     */
    public List<ChunkInfo> getChunkInfos() {
        return myChunkInfos;
    }

    /**
     * Finds the first connection whose MD5 sum matches the given value.  IF
     * there is no connection of that type, returns null.
     * There is no guarantee of exactly which Connection in the bag file will
     * be returned.
     *
     * @param msgMd5Sum The MD5 sum of the message type of the connection.
     * @return The first connection of that type, or null if none was found.
     */
    public Connection findFirstConnectionByMd5Sum(String msgMd5Sum) {
        for (Connection tmpConn : getConnections()) {
            if (tmpConn.getMd5sum().equals(msgMd5Sum)) {
                return tmpConn;
            }
        }

        return null;
    }

    /**
     * This is similar to {@link #forMessagesOfType(String, MessageHandler)},
     * except it will only process the messages on the first topic it finds
     * with the correct type.
     *
     * @param messageType The type of message, e.g. "sensor_msgs/NavSatFix"
     * @param handler     An object that will process that message.
     * @throws BagReaderException If there is an error reading the bag.
     */
    public void forFirstTopicWithMessagesOfType(String messageType, MessageHandler handler) throws BagReaderException {
        for (Connection conn : myConnectionsByType.get(messageType)) {
            try (FileChannel channel = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, conn, channel);

                while (iter.hasNext()) {
                    boolean keepWorking = handler.process(iter.next(), conn);
                    if (!keepWorking) {
                        return;
                    }
                }

                return;
            }
            catch (IOException e) {
                throw new BagReaderException(e);
            }
        }
    }

    /**
     * Iterates through every message with the given type and passes it to the
     * handler object.  This processes ALL messages of that type, regardless of
     * which topic or connection they arrived on.  Messages are not guaranteed
     * to be processed in any particular order.
     * If {@link MessageHandler#process(MessageType, Connection)} return false, it will
     * cease processing immediately.
     *
     * @param messageType The type of message, e.g. "sensor_msgs/NavSatFix"
     * @param handler     An object that will process that message.
     * @throws BagReaderException If there is an error reading the bag.
     */
    public void forMessagesOfType(String messageType, MessageHandler handler) throws BagReaderException {
        for (Connection conn : myConnectionsByType.get(messageType)) {
            try (FileChannel channel = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, conn, channel);

                while (iter.hasNext()) {
                    boolean keepWorking = handler.process(iter.next(), conn);
                    if (!keepWorking) {
                        return;
                    }
                }
            }
            catch (IOException e) {
                throw new BagReaderException(e);
            }
        }
    }

    /**
     * Iterates through every message published on the given topic and passes it
     * to the handler object.  Messages are not guaranteed to be processed in
     * any particular order.
     * If {@link MessageHandler#process(MessageType, Connection)} return false, it will
     * cease processing immediately.
     *
     * @param topic   The topic, e.g. "/localization/gps"
     * @param handler An object that will process that message.
     * @throws BagReaderException If there is an error reading the bag.
     */
    public void forMessagesOnTopic(String topic, MessageHandler handler) throws BagReaderException {
        for (Connection conn : myConnectionsByTopic.get(topic)) {
            try (FileChannel channel = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, conn, channel);

                while (iter.hasNext()) {
                    boolean keepWorking = handler.process(iter.next(), conn);
                    if (!keepWorking) {
                        return;
                    }
                }
            }
            catch (IOException e) {
                throw new BagReaderException(e);
            }
        }
    }

    /**
     * Searches through every connection in the bag for one with the specified
     * message type and returns the first message on that connection.
     *
     * @param messageType The message type; e.g., "sensor_msgs/NavSatFix"
     * @return The first message found of that type, or null if none were found.
     * @throws BagReaderException If there was an error reading the bag.
     */
    public MessageType getFirstMessageOfType(String messageType) throws BagReaderException {
        for (Connection conn : myConnectionsByType.get(messageType)) {
            try (FileChannel channel = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, conn, channel);
                if (iter.hasNext()) {
                    return iter.next();
                }
            }
            catch (IOException e) {
                throw new BagReaderException(e);
            }
        }

        return null;
    }

    /**
     * Searches through every connection in the bag for one on the specified
     * topic and returns the first message on that topic.
     * NOTE: There is no guarantee that this message will be the first one
     * physically located in the bag file or the first one that was recorded
     * chronologically.  It's simply the first one we can find.
     *
     * @param topic The topic to search for, e.g. "/localization/gps"
     * @return The first message found on that topic, or null if none were found.
     * @throws BagReaderException If there was an error reading the bag.
     */
    public MessageType getFirstMessageOnTopic(String topic) throws BagReaderException {
        for (Connection conn : myConnectionsByTopic.get(topic)) {
            try (FileChannel channel = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, conn, channel);
                if (iter.hasNext()) {
                    return iter.next();
                }
            }
            catch (IOException e) {
                throw new BagReaderException(e);
            }
        }

        return null;
    }

    /**
     * Gets all of the different types of messages in the bag file.
     * The keys in the multimap will be the message type's name
     * (e. g., "gps_common/GPSFix") and the values will be all of the
     * MD5Sums that were found for messages of that type (e. g.,
     * "3db3d0a7bc53054c67c528af84710b70").
     * <p>
     * Yes, a bag file will <i>probably</i> only have a single MD5 for any
     * given message type, but there's nothing stopping it from having
     * different types on different connections....
     *
     * @return All of the messages in the bag file.
     */
    public Multimap<String, String> getMessageTypes() {
        Multimap<String, String> mtMap = HashMultimap.create();
        List<Connection> connections = getConnections();
        for (Connection connection : connections) {
            mtMap.put(connection.getType(), connection.getMd5sum());
        }

        return mtMap;
    }

    /**
     * Returns all of the topics in the bag file.  The list is sorted by name.
     *
     * @return A list of topics in the bag file.
     * @throws BagReaderException if there is a problem
     */
    public List<TopicInfo> getTopics() throws BagReaderException {
        Map<String, TopicInfo> topicNameMap = new HashMap<>();
        Map<Integer, TopicInfo> topicConnIdMap = new HashMap<>();

        for (Connection conn : getConnections()) {
            TopicInfo topic = topicNameMap.get(conn.getTopic());
            if (topic == null) {
                topic = new TopicInfo(conn.getTopic(), conn.getType(), conn.getMd5sum());
                topicNameMap.put(conn.getTopic(), topic);
            }
            if (!topicConnIdMap.containsKey(conn.getConnectionId())) {
                topicConnIdMap.put(conn.getConnectionId(), topic);
            }
            topic.incrementConnectionCount();
        }

        if (!myIndexes.isEmpty()) {
            for (IndexData index : myIndexes) {
                TopicInfo info = topicConnIdMap.get(index.getConnectionId());
                if (info == null) {
                    throw new BagReaderException("IndexData referred to a connection ID (" +
                                                         index.getConnectionId() +
                                                         ") that was not found in the connection data.");
                }
                info.addToMessageCount(index.getCount());
            }
        }
        else {
            for (ChunkInfo info : myChunkInfos) {
                for (ChunkInfo.ChunkConnection conn : info.getConnections()) {
                    TopicInfo topic = topicConnIdMap.get(conn.getConnectionId());
                    if (topic == null) {
                        throw new BagReaderException("ChunkInfo referred to a connection ID (" +
                                                             conn.getConnectionId() +
                                                             ") that was not found in the connection data.");
                    }
                    topic.addToMessageCount(conn.getMessageCount());
                }
            }
        }

        List<TopicInfo> list = Lists.newArrayList(topicNameMap.values());
        Collections.sort(list);

        return list;
    }

    /**
     * If any chunks are compressed, this will return the most common type of
     * compression used in this bag file (either "lz4" or "bz2").  If no
     * chunks are compressed, this will return "none".
     * This will iterate through all of the Chunk and ChunkInfo records in a
     * bag, so it might be a little slow.
     *
     * @return The dominant compression type, "bz2" or "lz4", or "none" if there is none.
     */
    public String getCompressionType() {
        long lz4Count = 0;
        long bz2Count = 0;

        // First, check all of the Chunk records we read.
        for (Chunk chunk : myChunks) {
            String compression = chunk.getCompression();
            if (compression.equals("bz2")) {
                bz2Count++;
            }
            else if (compression.equals("lz4")) {
                lz4Count++;
            }
        }

        // It's possible we may not have actually read any Chunk records, just
        // ChunkInfo records that contain the position of Chunks.  In that
        // case, we have to go look up the chunks and read them.
        List<Long> chunkPositions = Lists.newArrayListWithExpectedSize(myChunkInfos.size());
        for (ChunkInfo info : myChunkInfos) {
            chunkPositions.add(info.getChunkPos());
        }
        try (FileChannel channel = getChannel()) {
            for (Long chunkPos : chunkPositions) {
                Chunk chunk = new Chunk(recordAt(channel, chunkPos));
                String compression = chunk.getCompression();
                if (compression.equals("bz2")) {
                    bz2Count++;
                }
                else if (compression.equals("lz4")) {
                    lz4Count++;
                }
            }
        }
        catch (IOException | BagReaderException e) {
            myLogger.warn("Error reading data chunk", e);
        }

        if (lz4Count > bz2Count) {
            return "lz4";
        }
        else if (bz2Count > lz4Count && bz2Count != 0) {
            return "bz2";
        }
        else {
            return "none";
        }
    }

    /**
     * Counts how many messages are in this bag file.  If this bag file is indexed,
     * it counts how many are listed in the indices; otherwise, it iterates through
     * chunks and connections to count how many are in there.
     *
     * @return The number of messages in the bag file.
     */
    public long getMessageCount() {
        long count = 0;
        if (!myIndexes.isEmpty()) {
            for (IndexData index : myIndexes) {
                count += index.getCount();
            }
        }
        else {
            for (ChunkInfo info : myChunkInfos) {
                for (ChunkInfo.ChunkConnection conn : info.getConnections()) {
                    count += conn.getMessageCount();
                }
            }
        }
        return count;
    }

    /**
     * Indicates whether this bag file has any indexes.
     *
     * @return "true" if there are indexes, "false" otherwise.
     */
    public boolean isIndexed() {
        return !myIndexes.isEmpty();
    }

    /**
     * Gets a message on the given topic at a particular index.
     * <p>
     * Messages are sorted in the order they were written to the bag file,
     * which may not be the same as their chronological order.
     *
     * @param topic The topic to get a message from.
     * @param index The index of the message in the topic.
     * @return The message at that position in the bag.
     * @throws BagReaderException             If there was an error reading the bag.
     * @throws ArrayIndexOutOfBoundsException If index is larger than the size of the index.
     */
    public MessageType getMessageOnTopicAtIndex(String topic,
                                                int index) throws BagReaderException {
        topic = topic.trim();
        List<MessageIndex> indexes = myMessageIndexesForTopics.get(topic);
        if (indexes == null) {
            generateIndexesForTopic(topic);
            indexes = myMessageIndexesForTopics.get(topic);
        }

        if (index >= indexes.size()) {
            throw new ArrayIndexOutOfBoundsException(index);
        }

        MessageType mt;
        try {
            Connection conn = myConnectionsByTopic.get(topic).iterator().next();
            mt = conn.getMessageCollection().getMessageType();
        }
        catch (UnknownMessageException e) {
            throw new BagReaderException(e);
        }

        try (FileChannel channel = getChannel()) {
            MessageIndex msgIndex = indexes.get(index);
            Record record = BagFile.recordAt(channel, msgIndex.fileIndex);
            ByteBufferChannel chunkChannel = new ByteBufferChannel(record.getData());
            Record message = BagFile.recordAt(chunkChannel, msgIndex.chunkIndex);
            mt.readMessage(message.getData().order(ByteOrder.LITTLE_ENDIAN));
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        return mt;
    }

    /**
     * Gets a message at a particular index.
     * <p>
     * Messages are sorted in the order they were written to the bag file, which
     * may not be the same as their chronological order.
     *
     * @param indexes the List of MessageIndex created by generateIndexesForTopic or generateIndexesForTopicList
     * @param index   The index of the message in the topic.
     * @return The message at that position in the bag.
     * @throws BagReaderException             If there was an error reading the bag.
     * @throws ArrayIndexOutOfBoundsException If index is larger than the size
     *                                        of the index.
     */
    public MessageType getMessageFromIndex(List<MessageIndex> indexes,
                                           int index) throws BagReaderException {

        if (index >= indexes.size()) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        String topic = indexes.get(index).topic;
        MessageType mt;
        try {
            Connection conn = myConnectionsByTopic.get(topic).iterator().next();
            mt = conn.getMessageCollection().getMessageType();
        }
        catch (UnknownMessageException e) {
            throw new BagReaderException(e);
        }

        try (FileChannel channel = getChannel()) {
            MessageIndex msgIndex = indexes.get(index);
            Record record = BagFile.recordAt(channel, msgIndex.fileIndex);
            ByteBufferChannel chunkChannel = new ByteBufferChannel(record.getData());
            Record message = BagFile.recordAt(chunkChannel, msgIndex.chunkIndex);
            mt.readMessage(message.getData().order(ByteOrder.LITTLE_ENDIAN));
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        return mt;
    }

    /**
     * Bags are supposed to have Index Data chunks that provided a convenient
     * mechanism for finding the indexes of individual messages within chunks.
     * In practice, they usually do not have this, so we have to iterate through
     * chunks in order to find the positions of individual messages.
     * This method builds up a list of indices for a particular topic and stores
     * it so that we don't have to look through it every time we want to find
     * a message.
     *
     * @param topic The topic to generate indices for.
     * @return the index, which is sorted according to BagFile.MessageIndex#compareTo
     * @throws BagReaderException If there was an error reading the bag file.
     */
    private List<MessageIndex> generateIndexesForTopic(String topic) throws BagReaderException {
        List<MessageIndex> msgIndexes = Lists.newArrayList();
        try (FileChannel channel = getChannel()) {
            for (Connection conn : myConnectionsByTopic.get(topic)) {
                for (ChunkInfo chunkInfo : myChunkInfosByConnectionId.get(conn.getConnectionId())) {
                    long chunkPos = chunkInfo.getChunkPos();

                    Record chunk = BagFile.recordAt(channel, chunkPos);
                    chunk.readData();
                    ByteBufferChannel chunkChannel = new ByteBufferChannel(chunk.getData());
                    while (chunkChannel.position() < chunkChannel.size()) {
                        long position = chunkChannel.position();
                        Record msg = new Record(chunkChannel);
                        msg.readData();
                        if (msg.getHeader().getType() == Record.RecordType.MESSAGE_DATA &&
                                msg.getHeader().getInt("conn") == conn.getConnectionId()) {
                            Timestamp t = msg.getHeader().getTimestamp("time");
                            msgIndexes.add(new MessageIndex(chunkPos, position, topic, t));
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        Collections.sort(msgIndexes);
        myMessageIndexesForTopics.put(topic, msgIndexes);
        return msgIndexes;
    }

    /**
     * Bags are supposed to have Index Data chunks that provided a convenient
     * mechanism for finding the indexes of individual messages within chunks.
     * In practice, they usually do not have this, so we have to iterate through
     * chunks in order to find the positions of individual messages.
     * This method builds up a list of indices for a list of topics and stores
     * it in Timestamp order (or fileIndex/chunkIndex order)
     * so that we don't have to look through it every time we want to find
     * a message.
     * <p>
     * If showProgressMonitor is true, then a caller can run generating the index on a background SwingWorker
     * thread. The SwingWorker progress is updated and the user can cancel the operation by using the ProgressMonitor Cancel button.
     * If the operation is canceled, an InterruptedException is generated.
     *
     * @param topics          The topics to generate indices for.
     * @param progressMonitor if non-null, progressMonitor is called with setProgress updates.
     *                        If a caller run the task in a SwingWorker thread that can
     *                        pop up a progress monitor on the root container window if the task takes a long time.
     * @return the index, which is sorted according to Timestamp
     * @throws BagReaderException             If there was an error reading the bag file.
     * @throws java.lang.InterruptedException if the operation is canceled via the ProgressMonitor
     * @see com.github.swrirobotics.bags.reader.BagFile.MessageIndex#compareTo(com.github.swrirobotics.bags.reader.BagFile.MessageIndex)
     */
    public List<MessageIndex> generateIndexesForTopicList(List<String> topics, ProgressMonitor progressMonitor) throws BagReaderException, InterruptedException {
        int totalNumChunks = 0, currentTotalChunkNum = 0; // used for progressMonitor
        if (progressMonitor != null) {
            for (String topic : topics) {
                Collection<Connection> connList = myConnectionsByTopic.get(topic);
                for (Connection conn : connList) {
                    Collection<ChunkInfo> chunkColl = myChunkInfosByConnectionId.get(conn.getConnectionId());
                    totalNumChunks += chunkColl.size();
                }
            }
            progressMonitor.setMinimum(0);
            progressMonitor.setMaximum(totalNumChunks);
            if (progressMonitor.isCanceled()) {
                throw new InterruptedException();
            }
        }
        List<MessageIndex> msgIndexes = Lists.newArrayList();
        try (FileChannel channel = getChannel()) {
            for (String topic : topics) {
                if (progressMonitor != null) {
                    progressMonitor.setNote("generating index for topic " + topic);
                }
                myLogger.info("generating index for topic " + topic);
                List<MessageIndex> msgIndex = Lists.newArrayList();
                int connNum = 0;
                Collection<Connection> connList = myConnectionsByTopic.get(topic);
                int numConns = connList.size();
                for (Connection conn : connList) {
                    myLogger.info("\tConnection " + conn + " (" + connNum + "/" + numConns + ")");
                    connNum++;
                    Collection<ChunkInfo> chunkColl = myChunkInfosByConnectionId.get(conn.getConnectionId());
                    int chunkNum = 0;
                    int numChunks = chunkColl.size();
                    for (ChunkInfo chunkInfo : chunkColl) {
                        long chunkPos = chunkInfo.getChunkPos();
                        if (chunkNum % 500 == 0) {
                            myLogger.info("\t\tChunk " + chunkNum + "/" + numChunks);
                        }
                        chunkNum++;
                        if (progressMonitor != null) {
                            if (progressMonitor.isCanceled()) {
                                progressMonitor.setNote("canceling");
                                throw new InterruptedException("canceled indexing");
                            }
                            progressMonitor.setProgress(currentTotalChunkNum++);
                        }
                        Record chunk = BagFile.recordAt(channel, chunkPos);
                        chunk.readData();
                        ByteBufferChannel chunkChannel = new ByteBufferChannel(chunk.getData());
                        while (chunkChannel.position() < chunkChannel.size()) {
                            long position = chunkChannel.position();
                            Record msg = new Record(chunkChannel);
                            msg.readData();
                            if (msg.getHeader().getType() == Record.RecordType.MESSAGE_DATA
                                    && msg.getHeader().getInt("conn") == conn.getConnectionId()) {
                                Timestamp t = msg.getHeader().getTimestamp("time");
                                msgIndex.add(new MessageIndex(chunkPos, position, topic, t));
                            }
                        }
                    }
                }
                myMessageIndexesForTopics.put(topic, msgIndex);
                msgIndexes.addAll(msgIndex);
            }
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        myLogger.info("sorting combined index with " + msgIndexes.size() + " messages");
        if (progressMonitor != null) {
            if (progressMonitor.isCanceled()) {
                throw new InterruptedException("canceled sorting index");
            }
            progressMonitor.setNote("sorting index");
            progressMonitor.setProgress(totalNumChunks);
        }
        Collections.sort(msgIndexes); // sort all messages by Timestamp
        if (progressMonitor != null) {
            progressMonitor.setNote("done sorting");
        }
        return msgIndexes;
    }

    /**
     * Checks whether or not the input channel is open and pointing at the beginning
     * of a V2.0 ROS bag file.  Throws an exception if it's not.
     *
     * @param myInput A byte channel that should be tested.
     * @throws BagReaderException If the byte channel is not at the beginning of a V2.0 bag file.
     */
    private void verifyBagFile(ReadableByteChannel myInput) throws BagReaderException {
        ByteBuffer buffer = ByteBuffer.allocate(13);
        int bytesRead;
        try {
            bytesRead = myInput.read(buffer);
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }
        if (bytesRead != 13) {
            throw new BagReaderException("Expected to read 13 bytes but only got " + bytesRead + ".");
        }

        String line = new String(buffer.array());
        if (!"#ROSBAG V2.0\n".equals(line)) {
            throw new BagReaderException("File did not start with the proper ROSBAG header.  " +
                                                 "Actual first 13 bytes: [" + line + "]");
        }
    }

    private boolean hasNext(SeekableByteChannel input) throws BagReaderException {
        try {
            return input.position() < input.size();
        }
        catch (IOException e) {
            throw new BagReaderException("Unable to count remaining bytes.");
        }
    }


    /**
     * Creates a new {@link Record} starting at a particular location in an input stream.
     * After this method is done, the position of the byte channel will be set to index.
     *
     * @param input The byte channel to get the record from.
     * @param index The index in the byte channel the record starts at.
     * @return A new record from that particular position.
     * @throws BagReaderException
     */
    static public Record recordAt(SeekableByteChannel input, long index) throws BagReaderException {
        try {
            input.position(index);
            return new Record(input);
        }
        catch (IOException e) {
            throw new BagReaderException("Unable to seek to position: " + index + "; caught exception " + e);
        }
    }

    /**
     * Generates a hash that acts as a fingerprint for this bag file.
     * <p>
     * Sometimes you'd like to be able to compare two bag files to determine if
     * they're identical.  Doing a byte-for-byte comparison or comparing MD5 sums
     * of bag files can be very slow for large bags.  This method generates a
     * hash that should be about as reliable as an MD5 sum of the entire bag file
     * but is much faster.
     * <p>
     * It does so by hashing data found in records throughout the file; it does
     * not compare the actual data inside data chunks.  In practice, this should
     * be good enough to uniquely identify any bag files generated in the real
     * world, and it will even uniquely identify bag files that have the same data
     * but have been reindexed or reordered.  It will not uniquely identify bag
     * files that have exactly the same record and header structure but differing
     * data inside their chunks.  I don't think that will ever happen unless
     * somebody manually crafts a bag file with differing chunks.
     * Please don't do that.
     *
     * @return An MD5 hash that can be used to uniquely identify this bag file.
     * @throws BagReaderException
     */
    public String getUniqueIdentifier() throws BagReaderException {
        MessageDigest digest = DigestUtils.getMd5Digest();

        if (myBagHeader == null) {
            read();
        }

        digest.update(Ints.toByteArray(myBagHeader.getChunkCount()));
        digest.update(Ints.toByteArray(myBagHeader.getConnCount()));
        digest.update(Longs.toByteArray(myBagHeader.getIndexPos()));

        for (Chunk chunk : getChunks()) {
            digest.update(chunk.getCompression().getBytes());
            digest.update(Ints.toByteArray(chunk.getSize()));
        }

        for (Connection conn : getConnections()) {
            if (conn.getCallerId() != null) {
                digest.update(conn.getCallerId().getBytes());
            }
            digest.update(Ints.toByteArray(conn.getConnectionId()));
            digest.update(conn.getMd5sum().getBytes());
            digest.update(conn.getTopic().getBytes());
            digest.update(conn.getMessageDefinition().getBytes());
        }

        for (MessageData data : getMessages()) {
            digest.update(Ints.toByteArray(data.getConnectionId()));
            digest.update(Longs.toByteArray(data.getTime().getTime()));
        }

        for (IndexData indexData : getIndexes()) {
            digest.update(Ints.toByteArray(indexData.getConnectionId()));
            digest.update(Ints.toByteArray(indexData.getCount()));
            for (IndexData.Index index : indexData.getIndexes()) {
                digest.update(Longs.toByteArray(index.getTime().getTime()));
                digest.update(Ints.toByteArray(index.getOffset()));
            }
        }

        for (ChunkInfo chunk : getChunkInfos()) {
            digest.update(Longs.toByteArray(chunk.getChunkPos()));
            digest.update(Ints.toByteArray(chunk.getCount()));
            digest.update(Longs.toByteArray(chunk.getEndTime().getTime()));
            digest.update(Longs.toByteArray(chunk.getStartTime().getTime()));
            for (ChunkInfo.ChunkConnection conn : chunk.getConnections()) {
                digest.update(Ints.toByteArray(conn.getConnectionId()));
                digest.update(Ints.toByteArray(conn.getMessageCount()));
            }
        }

        StringBuilder builder = new StringBuilder();

        byte[] md5sum = digest.digest();
        for (byte b : md5sum) {
            builder.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
        }

        return builder.toString();
    }

    /**
     * Reads all of the records in a bag file into this object.
     * This method must be called before attempting to extract any data from
     * the bag file.  {@link BagReader#readFile(File)} and {@link BagReader#readFile(String)}
     * will automatically do that for you; if you create a BagFile using
     * {@link #BagFile(String)}, you must manually call this.
     * This will only read a bag file once.  Successive calls to this method
     * will have no effect.
     *
     * @throws BagReaderException
     */
    public void read() throws BagReaderException {
        if (myBagHeader != null) {
            return;
        }

        try (FileChannel input = getChannel()) {
            verifyBagFile(input);

            while (hasNext(input)) {
                Record record = new Record(input);
                switch (record.getHeader().getType()) {
                    case BAG_HEADER:
                        this.myBagHeader = new BagHeader(record);
                        if (this.getBagHeader().getIndexPos() == 0) {
                            throw new BagReaderException(
                                    "Unable to read bag header; reindex the bag file.");
                        }
                        input.position(this.getBagHeader().getIndexPos());
                        break;
                    case CHUNK:
                        myPreviousChunk = new Chunk(record);
                        this.getChunks().add(myPreviousChunk);
                        break;
                    case CONNECTION:
                        record.setConnectionHeader(new Header(record.getData()));
                        this.addConnection(new Connection(record));
                        break;
                    case MESSAGE_DATA:
                        this.getMessages().add(new MessageData(record));
                        break;
                    case INDEX_DATA:
                        if (myPreviousChunk == null) {
                            throw new BagReaderException(
                                    "No chunk found for index at position " + input.position());
                        }
                        this.getIndexes().add(new IndexData(record, myPreviousChunk));
                        break;
                    case CHUNK_INFO:
                        this.addChunkInfo(new ChunkInfo(record));
                        break;
                    default:
                        throw new BagReaderException("Unknown header type.");
                }
            }
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        for (ChunkInfo info : this.myChunkInfos) {
            this.myStartTime = minTimestamp(this.myStartTime, info.getStartTime());
            this.myStartTime = minTimestamp(this.myStartTime, info.getEndTime());
            this.myEndTime = maxTimestamp(this.myEndTime, info.getStartTime());
            this.myEndTime = maxTimestamp(this.myEndTime, info.getEndTime());
        }

        for (IndexData indexData : this.myIndexes) {
            for (IndexData.Index index : indexData.getIndexes()) {
                this.myStartTime = minTimestamp(this.myStartTime, index.getTime());
                this.myEndTime = maxTimestamp(this.myEndTime, index.getTime());
            }
        }

        if (this.getStartTime() != null &&
                this.getEndTime() != null &&
                this.getStartTime().getTime() != 0 &&
                this.getEndTime().getTime() != 0) {
            this.myDurationS = ((double) (this.getEndTime().getTime() - this.getStartTime().getTime())) / 1000.0;
        }
        else {
            myLogger.warn("No chunk info records found; start and end time are unknown.");

            myLogger.warn("Record type counts:" +
                                  "\n  Header: " + (this.getBagHeader() == null ? 0 : 1) +
                                  "\n  Chunk: " + this.getChunks().size() +
                                  "\n  Connection: " + this.getConnections().size() +
                                  "\n  Message Data: " + this.getMessages().size() +
                                  "\n  Index Data: " + this.getIndexes().size() +
                                  "\n  Chunk Info: " + this.getChunkInfos().size());
        }

        if (this.getBagHeader().getChunkCount() !=
                (this.getChunks().size() + this.getChunkInfos().size()) ||
                this.getBagHeader().getConnCount() != this.getConnections().size()) {
            String errorMsg = "Expected " + this.getBagHeader().getChunkCount() +
                    " chunks and " + this.getBagHeader().getConnCount() +
                    " connections, but got " +
                    (this.getChunks().size() + this.getChunkInfos().size()) + " and " +
                    this.getConnections().size() + ".  This is ok if the file " +
                    "is in the middle of being written to disk, otherwise the bag " +
                    "may be corrupt.";
            myLogger.warn(errorMsg);
            throw new BagReaderException(errorMsg);
        }
    }

    /**
     * Compares two timestamps and returns the smallest value.  If one value is
     * null, it will return the other value.
     *
     * @param left  First timestamp to compare.
     * @param right Second timestamp to compare.
     * @return The smallest value.
     */
    private static Timestamp minTimestamp(Timestamp left, Timestamp right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        if (left.compareTo(right) < 0) {
            return left;
        }
        return right;
    }

    /**
     * Compares two timestamps and returns the largest value.  If one value is
     * null, it will return the other value.
     *
     * @param left  First timestamp to compare.
     * @param right Second timestamp to compare.
     * @return The largest value.
     */
    private static Timestamp maxTimestamp(Timestamp left, Timestamp right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        if (left.compareTo(right) > 0) {
            return left;
        }
        return right;
    }

    /**
     * Prints a block of text listing various bits of metadata about a bag
     * including its duration, start and end times, size, number of messages,
     * and the number of different message types and topics used.  It's fairly
     * similar to the output of "rosbag info ..."
     *
     * @throws BagReaderException
     */
    public void printInfo() throws BagReaderException {
        myLogger.info("Version:     " + this.getVersion());
        myLogger.info("Compression: " + this.getCompressionType());
        myLogger.info("Duration:    " + this.getDurationS() + "s");
        myLogger.info("Start:       " + (this.getStartTime() == null ?
                "Unknown" : (this.getStartTime().toString() + " (" + this.getStartTime().getTime() + ")")));
        myLogger.info("End:         " + (this.getEndTime() == null ?
                "Unknown" : (this.getEndTime().toString() + " (" + this.getEndTime().getTime() + ")")));
        myLogger.info("Size:        " +
                              (((double) this.getPath().toFile().length()) / 1024.0) + " MB");
        myLogger.info("Messages:    " + this.getMessageCount());
        myLogger.info("Types:");
        for (Map.Entry<String, String> entry : this.getMessageTypes().entries()) {
            myLogger.info("  " + entry.getKey() + " \t\t[" + entry.getValue() + "]");
        }
        myLogger.info("Topics:");
        for (TopicInfo topic : this.getTopics()) {
            myLogger.info("  " + topic.getName() + " \t\t" + topic.getMessageCount() +
                                  " msgs \t: " + topic.getMessageType() + " \t" +
                                  (topic.getConnectionCount() > 1 ? ("(" + topic.getConnectionCount() + " connections)") : ""));
        }
    }
}
