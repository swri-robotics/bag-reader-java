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
import com.github.swrirobotics.bags.reader.messages.serialization.MessageType;
import com.github.swrirobotics.bags.reader.records.*;
import com.google.common.collect.Multimap;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.List;

@SuppressWarnings("unused")
public interface BagFile {
    /**
     * Opens a read-only SeekableByteChannel that refers to the underlying bag file.
     *
     * @return An open SeekableByteChannel.
     * @throws IOException If there is an error opening the file.
     */
    SeekableByteChannel getChannel() throws IOException;

    /**
     * Gets the path to the bag file.
     *
     * @return The path to the bag file.
     */
    Path getPath();

    /**
     * The version of the bag file.  Currently, this will always be 2.0.
     *
     * @return 2.0
     */
    String getVersion();

    /**
     * The amount of time in seconds that the bag file spans.  If the start
     * time and end time could be read, this will be the difference between
     * them; otherwise it will be 0.0.
     *
     * @return The amount of time in seconds that the bag file spans.
     */
    double getDurationS();

    /**
     * The earliest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a start time.
     *
     * @return The bag's start time.
     */
    Timestamp getStartTime();

    /**
     * The latest time of any chunk in the bag file.  This may be null if
     * the bag file had no chunks or if none of them had a end time.
     *
     * @return The bag's end time.
     */
    Timestamp getEndTime();

    /**
     * The bag's header record.
     *
     * @return The bag's header.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Bag_header">http://wiki.ros.org/Bags/Format/2.0#Bag_header</a>
     */
    BagHeader getBagHeader();

    /**
     * All the chunks in the bag file.
     *
     * @return All the chunks in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk">http://wiki.ros.org/Bags/Format/2.0#Chunk</a>
     */
    List<Chunk> getChunks();

    /**
     * All the connections in the bag file.
     *
     * @return All the connections in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Connection">http://wiki.ros.org/Bags/Format/2.0#Connection</a>
     */
    List<Connection> getConnections();

    /**
     * All the messages in the bag file.
     *
     * @return All the messages in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Message_data">http://wiki.ros.org/Bags/Format/2.0#Message_data</a>
     */
    List<MessageData> getMessages();

    /**
     * All the indexes in the bag file.
     *
     * @return All the indexes in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Index_data">http://wiki.ros.org/Bags/Format/2.0#Index_data</a>
     */
    List<IndexData> getIndexes();

    /**
     * All the chunk infos in the bag file.
     *
     * @return All the chunk infos in the bag file.
     * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Chunk_info">http://wiki.ros.org/Bags/Format/2.0#Chunk_info</a>
     */
    List<ChunkInfo> getChunkInfos();

    /**
     * This is similar to {@link #forMessagesOfType(String, MessageHandler)},
     * except it will only process the messages on the first topic it finds
     * with the correct type.
     *
     * @param messageType The type of message, e.g. "sensor_msgs/NavSatFix"
     * @param handler     An object that will process that message.
     * @throws BagReaderException If there is an error reading the bag.
     */
    void forFirstTopicWithMessagesOfType(String messageType, MessageHandler handler) throws BagReaderException;

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
    void forMessagesOfType(String messageType, MessageHandler handler) throws BagReaderException;

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
    void forMessagesOnTopic(String topic, MessageHandler handler) throws BagReaderException;

    /**
     * Iterates through every message published on a given connection and passes it to the handler object.
     * @param conn The connection.
     * @param handler An object that will process that message.
     * @throws BagReaderException If there is an error reading the bag.
     */
    void forMessagesOnConnection(Connection conn, MessageHandler handler) throws BagReaderException;

    /**
     * Searches through every connection in the bag for one with the specified
     * message type and returns the first message on that connection.
     *
     * @param messageType The message type; e.g., "sensor_msgs/NavSatFix"
     * @return The first message found of that type, or null if none were found.
     * @throws BagReaderException If there was an error reading the bag.
     */
    MessageType getFirstMessageOfType(String messageType) throws BagReaderException;

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
    MessageType getFirstMessageOnTopic(String topic) throws BagReaderException;

    /**
     * Gets the first message on the given connection.  Returns null if there are no messages.
     * @param conn The connection.
     * @return The first message on the connection or null if there are none.
     * @throws BagReaderException If there was an error reading the bag.
     */
    MessageType getFirstMessageOnConnection(Connection conn) throws BagReaderException;

    /**
     * Gets all the different types of messages in the bag file.
     * The keys in the multimap will be the message type's name
     * (e. g., "gps_common/GPSFix") and the values will be All the
     * MD5Sums that were found for messages of that type (e. g.,
     * "3db3d0a7bc53054c67c528af84710b70").
     * <p>
     * Yes, a bag file will <i>probably</i> only have a single MD5 for any
     * given message type, but there's nothing stopping it from having
     * different types on different connections....
     *
     * @return All the messages in the bag file.
     */
    Multimap<String, String> getMessageTypes();

    /**
     * Returns all the topics in the bag file.  The list is sorted by name.
     *
     * @return A list of topics in the bag file.
     * @throws BagReaderException if there is a problem
     */
    List<TopicInfo> getTopics() throws BagReaderException;

    /**
     * If any chunks are compressed, this will return the most common type of
     * compression used in this bag file (either "lz4" or "bz2").  If no
     * chunks are compressed, this will return "none".
     * This will iterate through All the Chunk and ChunkInfo records in a
     * bag, so it might be a little slow.
     *
     * @return The dominant compression type, "bz2" or "lz4", or "none" if there is none.
     */
    String getCompressionType();

    /**
     * Counts how many messages are in this bag file.  If this bag file is indexed,
     * it counts how many are listed in the indices; otherwise, it iterates through
     * chunks and connections to count how many are in there.
     *
     * @return The number of messages in the bag file.
     */
    long getMessageCount();

    /**
     * Indicates whether this bag file has any indexes.
     *
     * @return "true" if there are indexes, "false" otherwise.
     */
    boolean isIndexed();

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
    MessageType getMessageOnTopicAtIndex(String topic,
                                                int index) throws BagReaderException;

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
    MessageType getMessageFromIndex(List<BagFileImpl.MessageIndex> indexes,
                                           int index) throws BagReaderException;

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
     */
    List<BagFileImpl.MessageIndex> generateIndexesForTopicList(List<String> topics, ProgressMonitor progressMonitor) throws BagReaderException, InterruptedException;

    /**
     * Creates a new {@link Record} starting at a particular location in an input stream.
     * After this method is done, the position of the byte channel will be set to index.
     *
     * @param input The byte channel to get the record from.
     * @param index The index in the byte channel the record starts at.
     * @return A new record from that particular position.
     * @throws BagReaderException If there was an error seeking to the record.
     */
    static Record recordAt(SeekableByteChannel input, long index) throws BagReaderException {
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
     * @throws BagReaderException If there wasn an error reading the bag file.
     */
    String getUniqueIdentifier() throws BagReaderException;

    /**
     * Reads all the records in a bag file into this object.
     * This method must be called before attempting to extract any data from
     * the bag file.  {@link BagReader#readFile(File)} and {@link BagReader#readFile(String)}
     * will automatically do that for you; if you create a BagFile using
     * the constructor, you must manually call this.
     * This will only read a bag file once.  Successive calls to this method
     * will have no effect.
     *
     * @throws BagReaderException If there was an error reading the bag's headers.
     */
    void read() throws BagReaderException;

    /**
     * Prints a block of text listing various bits of metadata about a bag
     * including its duration, start and end times, size, number of messages,
     * and the number of different message types and topics used.  It's fairly
     * similar to the output of "rosbag info ..."
     *
     * @throws BagReaderException If there was an error reading the bag file.
     */
    void printInfo() throws BagReaderException;
}
