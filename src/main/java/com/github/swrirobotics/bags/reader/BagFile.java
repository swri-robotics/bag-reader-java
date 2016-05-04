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
import com.github.swrirobotics.bags.reader.exceptions.UninitializedFieldException;
import com.github.swrirobotics.bags.reader.messages.serialization.*;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private final List<MessageData> myMessages = Lists.newArrayList();
    private final List<IndexData> myIndexes = Lists.newArrayList();
    private final List<ChunkInfo> myChunkInfos = Lists.newArrayList();

    private static final String[]
            GPS_TYPE_MD5_SUMS = new String[] {"3db3d0a7bc53054c67c528af84710b70", // gps_common/GPSFix
                                              "2d3a8cd499b9b4a0249fb98fd05cfa48", // sensor_msgs/NavSatFix
                                              "e9eea369cace13aae0ef3e5a7c6b0ff6" // marti_gps_common/GPSFix
    };

    private static final String[] GPS_TOPICS = new String[] {"/localization/gps",
                                                             "gps",
                                                             "/vehicle/gps/fix",
                                                             "/localization/sensors/gps/novatel/raw",
                                                             "/localization/sensors/gps/novatel/fix",
                                                             "/imu_3dm_node/gps/fix",
                                                             "/local_xy_origin"
    };

    private static final Logger myLogger = LoggerFactory.getLogger(BagFile.class);

    /**
     * Wrapper class used to represent GPS positions and their timestamps
     * extracted from a bag file.
     */
    public static class GpsPositions {
        public List<Double[]> positions = Lists.newArrayList();
        public List<Timestamp> timestamps = Lists.newArrayList();
    }

    /**
     * Constructs a new BagFile that represents a bag at the given path.
     * If you create a bag file using this constructor, you should then call
     * {@link #read()} to read the bag's data into memory.
     * @param filePath The path of the file to open.
     */
    public BagFile(String filePath) {
        this.myPath = FileSystems.getDefault().getPath(filePath);
    }

    private SeekableByteChannel getChannel() throws IOException {
        return FileChannel.open(getPath(), StandardOpenOption.READ);
    }

    /**
     * Gets the path to the bag file.  This will be the same as the string that
     * was passed to {@link #BagFile(String)}.
     * @return The path to the bag file.
     */
    public Path getPath() {
        return myPath;
    }

    /**
     * The version of the bag file.  Currently, this will always be 2.0.
     * @return 2.0
     */
    public String getVersion() {
        return version;
    }

    /**
     * The amount of time in seconds that the bag file spans.  If the start
     * time and end time could be read, this will be the difference between
     * them; otherwise it will be 0.0.
     * @return The amount of time in seconds that the bag file spans.
     */
    public double getDurationS() {
        return myDurationS;
    }

    /**
     * The earliest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a start time.
     * @return The bag's start time.
     */
    public Timestamp getStartTime() {
        return myStartTime;
    }

    /**
     * The latest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a end time.
     * @return The bag's end time.
     */
    public Timestamp getEndTime() {
        return myEndTime;
    }

    /**
     * The bag's header record.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Bag_header">http://wiki.ros.org/Bags/Format/2.0#Bag_header</a>
     * @return The bag's header.
     */
    public BagHeader getBagHeader() {
        return myBagHeader;
    }

    /**
     * All of the chunks in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk">http://wiki.ros.org/Bags/Format/2.0#Chunk</a>
     * @return All of the chunks in the bag file.
     */
    public List<Chunk> getChunks() {
        return myChunks;
    }

    /**
     * All of the connections in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Connection">http://wiki.ros.org/Bags/Format/2.0#Connection</a>
     * @return All of the connections in the bag file.
     */
    public List<Connection> getConnections() {
        return myConnections;
    }

    /**
     * Convenience method for adding a new connection; updates both the internal
     * list and id map.
     * @param connection The connection to add.
     */
    private void addConnection(Connection connection) {
        myConnections.add(connection);
        myConnectionsById.put(connection.getConnectionId(), connection);
    }

    /**
     * All of the messages in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Message_data">http://wiki.ros.org/Bags/Format/2.0#Message_data</a>
     * @return All of the messages in the bag file.
     */
    public List<MessageData> getMessages() {
        return myMessages;
    }

    /**
     * All of the indexes in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Index_data">http://wiki.ros.org/Bags/Format/2.0#Index_data</a>
     * @return All of the indexes in the bag file.
     */
    public List<IndexData> getIndexes() {
        return myIndexes;
    }

    /**
     * All of the chunk infos in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk_info">http://wiki.ros.org/Bags/Format/2.0#Chunk_info</a>
     * @return All of the chunk infos in the bag file.
     */
    public List<ChunkInfo> getChunkInfos() {
        return myChunkInfos;
    }

    /**
     * Finds all of the connections made to a particular topic with a
     * particular MD5 sum.
     *
     * You're thinking, "but this is silly, all messages on a topic should have
     * the same MD5 sum!"  This is true within a single bag file, but different
     * bag files could have topics with the same name but different messages.  If
     * you are searching through multiple bag files for particular messages on a
     * particular topic, you need to ensure that you only get the messages for the
     * type that you really want.
     * @param topic The topic name to search for.
     * @param msgMd5Sum The MD5 sum of messages sent on that topic.
     * @return All of the connections made with that topic/md5sum.
     */
    public List<Connection> findAllConnectionsOnTopic(String topic, String msgMd5Sum) {
        List<Connection> conns = Lists.newArrayList();
        conns.addAll(getConnections().parallelStream().filter(conn -> conn.getTopic().equals(topic) &&
                                                              conn.getMd5sum().equals(msgMd5Sum))
                                     .collect(Collectors.toList()));
        return conns;
    }

    /**
     * Returns the connection ID of the first connection  a particular topic.
     * Returns -1 if there were no connections on that topic.
     * There is no guarantee of exactly which connection in the bag file will
     * be found.
     * @param topic A ntopic name to search for.
     * @return A connection ID if one was found on that topic; -1 otherwise.
     */
    private int findFirstConnectionOnTopic(String topic) {
        int connId = -1;
        for (Connection conn : getConnections()) {
            if (conn.getTopic().equals(topic)) {
                connId = conn.getConnectionId();
                break;
            }
        }

        return connId;
    }

    /**
     * Finds the first connection whose MD5 sum matches the given value.  IF
     * there is no connection of that type, returns null.
     * There is no guarantee of exactly which Connection in the bag file will
     * be returned.
     * @param msgMd5Sum The MD5 sum of the message type of the connection.
     * @return The first connection of that type, or null if none was found.
     */
    public Connection findFirstConnectionOfType(String msgMd5Sum) {
        for (Connection tmpConn : getConnections()) {
            if (tmpConn.getMd5sum().equals(msgMd5Sum)) {
                return tmpConn;
            }
        }

        return null;
    }

    private Record getFirstChunkForConnection(int connId, SeekableByteChannel input) throws BagReaderException {
        long chunkPos = -1;
        for (ChunkInfo info : myChunkInfos) {
            for (ChunkInfo.ChunkConnection conn : info.getConnections()) {
                if (conn.getConnectionId() == connId) {
                    chunkPos = info.getChunkPos();
                    break;
                }
            }
        }
        if (chunkPos == -1) {
            throw new BagReaderException("Connection not found.");
        }

        Record chunk = recordAt(input, chunkPos);
        chunk.readData();

        return chunk;
    }

    private ByteBuffer findFirstMessageForConnection(int connId) throws BagReaderException {
        Record chunk;
        try (SeekableByteChannel input = getChannel()) {
            chunk = getFirstChunkForConnection(connId, input);
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        final ByteBufferChannel data = new ByteBufferChannel(chunk.getData());
        try {
            while (data.position() < data.size()) {
                Record record = new Record(data);
                if (record.getHeader().getType() == Record.RecordType.MESSAGE_DATA) {
                    if (record.getHeader().getInt("conn") == connId) {
                        return record.getData().order(ByteOrder.LITTLE_ENDIAN);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new BagReaderException(e);
        }

        return null;
    }

    /**
     * Attempts to find all of the GPS messages in the bag file and return
     * a list containing all of them and all of the timestamps associated
     * with them.
     * @return All of the GPS positions in the bag file.
     * @throws BagReaderException
     */
    public GpsPositions getAllGpsMessages() throws BagReaderException {
        List<Connection> connIds = Lists.newArrayList();

        // First, check all of our known GPSFix topics for any messages.
        for (String topic : GPS_TOPICS) {
            for (String md5sum : GPS_TYPE_MD5_SUMS) {
                List<Connection> tmpConnIds = findAllConnectionsOnTopic(topic, md5sum);
                if (!tmpConnIds.isEmpty()) {
                    connIds.addAll(tmpConnIds);
                    break;
                }
            }
            if (!connIds.isEmpty()) {
                myLogger.debug("Found " + connIds.size() +
                               " GPS connections on topic " + topic + ".");
                break;
            }
        }

        GpsPositions wrapper = new GpsPositions();

        if (!connIds.isEmpty()) {
            wrapper.positions = Lists.newArrayList();
            wrapper.timestamps = Lists.newArrayList();

            try (SeekableByteChannel input = getChannel()) {
                MsgIterator iter = new MsgIterator(myChunkInfos, connIds, input);
                while (iter.hasNext()) {
                    MessageType msg = iter.next();
                    try {
                        double longitude = ((Float64Type) msg.getField("longitude")).getValue();
                        double latitude = ((Float64Type) msg.getField("latitude")).getValue();
                        wrapper.positions.add(new Double[]{longitude, latitude});
                        Field tsField = ((MessageType) msg.getField("header")).getField("stamp");
                        if (tsField instanceof TimeType) {
                            Timestamp time = ((TimeType) tsField).getValue();
                            wrapper.timestamps.add(time);
                        }
                    }
                    catch (UninitializedFieldException e) {
                        throw new BagReaderException(e);
                    }
                }

                if (wrapper.timestamps.size() != wrapper.positions.size()) {
                    throw new BagReaderException("Could not read timestamps for every GPS value.");
                }
            }
            catch (IOException e) {
                myLogger.error("Error opening channel:", e);
            }
        }

        return wrapper;
    }

    /**
     * Finds the first GPS message that it can in the bag file and returns it
     * as a [latitude, longitude] pair.  There's no guarantee as to exactly
     * which GPS message this will find in the bag file.
     * @return The first GPS message in the bag file; null if none is found.
     * @throws BagReaderException
     */
    public Double[] getFirstGpsMessage() throws BagReaderException {
        List<Connection> conns = Lists.newArrayList();
        for (String md5sum : GPS_TYPE_MD5_SUMS) {
            Connection conn = findFirstConnectionOfType(md5sum);
            if (conn != null) {
                conns.add(conn);
            }
        }

        if (!conns.isEmpty()) {
            for (Connection conn : conns) {
                try (SeekableByteChannel input = getChannel()) {
                    MsgIterator iter = new MsgIterator(myChunkInfos, conn, input);
                    MessageType mt = iter.next();
                    if (mt != null) {
                        double lat = ((Float64Type) mt.getField("latitude")).getValue();
                        double lon = ((Float64Type) mt.getField("longitude")).getValue();
                        return new Double[]{lat, lon};
                    }
                }
                catch (IOException | UninitializedFieldException e) {
                    throw new BagReaderException(e);
                }
            }
        }

        return null;
    }

    /**
     * We have all of our vehicles publish their names as a string on the topic
     * "/vms/vehicle_name".  This is just a convenient method for getting that
     * string.
     * @return The first string published to "/vms/vehicle_name".
     * @throws BagReaderException
     */
    public String getVehicleName() throws BagReaderException {
        int connId = findFirstConnectionOnTopic("/vms/vehicle_name");

        if (connId != -1) {
            ByteBuffer nameData = findFirstMessageForConnection(connId);

            if (nameData != null) {
                byte[] bytes = nameData.array();
                String name = new String(bytes, Charset.forName("UTF-8"));
                // For some reason, sometimes the vehicle names have
                // non-printable characters in them.
                // Trim whitespace off of the beginning and end and strip out
                // any non-printable characters.
                return name.replaceAll("\\p{C}", "").trim();
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
     *
     * Yes, a bag file will <i>probably</i> only have a single MD5 for any
     * given message type, but there's nothing stopping it from having
     * different types on different connections....
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
     * @return A list of topics in the bag file.
     * @throws BagReaderException
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
     * Counts how many messages are in this bag file.  If this bag file is indexed,
     * it counts how many are listed in the indices; otherwise, it iterates through
     * chunks and connections to count how many are in there.
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
     * @return "true" if there are indexes, "false" otherwise.
     */
    public boolean isIndexed() {
        return !myIndexes.isEmpty();
    }

    /**
     * Checks whether or not the input channel is open and pointing at the beginning
     * of a V2.0 ROS bag file.  Throws an exception if it's not.
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
            throw new BagReaderException("Unable to seek to position: " + index);
        }
    }

    /**
     * Generates a hash that acts as a fingerprint for this bag file.
     *
     * Sometimes you'd like to be able to compare two bag files to determine if
     * they're identical.  Doing a byte-for-byte comparison or comparing MD5 sums
     * of bag files can be very slow for large bags.  This method generates a
     * hash that should be about as reliable as an MD5 sum of the entire bag file
     * but is much faster.
     *
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
            if (conn.getCallerId() != null ) {
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
     * This will only read a bag file ones.  Successive calls to this method
     * will have no effect.
     * @throws BagReaderException
     */
    public void read() throws BagReaderException {
        if (myBagHeader != null) {
            return;
        }

        try (SeekableByteChannel input = getChannel()){
            verifyBagFile(input);

            while (hasNext(input)) {
                Record record = new Record(input);
                switch (record.getHeader().getType()) {
                    case BAG_HEADER:
                        this.myBagHeader = new BagHeader(record);
                        if (this.getBagHeader().getIndexPos() == 0) {
                            throw new BagReaderException("Unable to read bag header; reindex the bag file.");
                        }
                        input.position(this.getBagHeader().getIndexPos());
                        break;
                    case CHUNK:
                        this.getChunks().add(new Chunk(record));
                        break;
                    case CONNECTION:
                        record.setConnectionHeader(new Header(record.getData()));
                        this.addConnection(new Connection(record));
                        break;
                    case MESSAGE_DATA:
                        this.getMessages().add(new MessageData(record));
                        break;
                    case INDEX_DATA:
                        this.getIndexes().add(new IndexData(record));
                        break;
                    case CHUNK_INFO:
                        this.getChunkInfos().add(new ChunkInfo(record));
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
            if (this.getStartTime() == null ||
                    info.getStartTime().compareTo(this.getStartTime()) < 0) {
                this.myStartTime = info.getStartTime();
            }
            if (this.getEndTime() == null ||
                    info.getEndTime().compareTo(this.getEndTime()) > 0) {
                this.myEndTime = info.getEndTime();
            }
        }

        if (this.getStartTime() != null && this.getEndTime() != null) {
            this.myDurationS = ((double) (this.getEndTime().getTime() - this.getStartTime().getTime())) / 1000.0;
        }
        else {
            myLogger.warn("No chunk info records found; start and end time are unknown.");
        }
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
        myLogger.info("Version:  " + this.getVersion());
        myLogger.info("Duration: " + this.getDurationS() + "s");
        myLogger.info("Start:    " + (this.getStartTime() == null ?
                "Unknown" : (this.getStartTime().toString() + " (" + this.getStartTime().getTime() + ")")));
        myLogger.info("End:      " + (this.getEndTime() == null ?
                "Unknown" : (this.getEndTime().toString() + " (" + this.getEndTime().getTime() + ")")));
        myLogger.info("Size:     " +
                            (((double) this.getPath().toFile().length()) / 1024.0) + " MB");
        myLogger.info("Messages: " + this.getMessageCount());
        myLogger.info("Types:    ");
        for (Map.Entry<String, String> entry : this.getMessageTypes().entries()) {
            myLogger.info("  " + entry.getKey() + " \t\t[" + entry.getValue() + "]");
        }
        myLogger.info("Topics:");
        for (TopicInfo topic : this.getTopics()) {
            myLogger.info("  " + topic.getName() + " \t\t" + topic.getMessageCount() +
                                " msgs \t: " + topic.getMessageType() + " \t" +
                                (topic.getConnectionCount() > 1 ? ("(" + topic.getConnectionCount() + " connections)") : ""));
        }

        String name = this.getVehicleName();
        myLogger.info("Vehicle Name: " + (name == null ? "(none)" : name));
    }
}
