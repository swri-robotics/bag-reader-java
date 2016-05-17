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

import com.github.swrirobotics.bags.reader.exceptions.InvalidDefinitionException;
import com.github.swrirobotics.bags.reader.exceptions.UnknownMessageException;
import com.github.swrirobotics.bags.reader.records.Connection;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * This represents a collection of interdependent message definitions and can
 * be used for deserialization any of those messages from their byte format.
 *
 * Typically, one MessageCollection is created per
 * {@link Connection} in a bag file.
 * Each connection record has a header that contains the definition of the
 * type of message transmitted over that connection as well as all of the
 * definitions for any messages that one depends on.  That means it is possible
 * to deserialize all of the data on that connection even if you only have the
 * connection headers.
 *
 * To use this class:
 * 1) Create a new instance of MessageCollection.
 *    If you have a {@link Connection}, then you can
 *    simply call {@link Connection#getMessageCollection()}.
 *    If you need to build one from scratch:
 *    a) Call {@link #setTopType(String)} to set the type of message transmitted
 *       on that connection; for example, "gps_common/GPSStatus"
 *    b) Call {@link #parseMessages(String)} and pass in the entire message
 *       definition string from the connection header.
 * 2) Now you can call any of the getMessageType methods to get a
 *    {@link MessageType} object that can deserialize messages of the appropriate
 *    type.
 */
public class MessageCollection {
    // Maps message names without packages ("GPSFix") to their MessageType
    private final Map<String, MessageType> myMsgTypesByName = Maps.newHashMap();
    // Maps package names and message names to their MessageType
    private final Map<String, Map<String, MessageType>>  myMsgTypesByPackage = Maps.newHashMap();
    // Maps message MD5 Sums to their MessageType
    private final Map<String, MessageType> myMsgTypeByMd5Sum = Maps.newHashMap();
    // The type of message that is intended to be transmitted over the
    // Connection this collection is associated with
    private String myTopType = null;

    // Splits message definitions within a connection header
    private final Splitter MSG_SPLITTER = Splitter.on("================================================================================").trimResults().omitEmptyStrings();

    private static final Logger myLogger = LoggerFactory.getLogger(MessageCollection.class);

    /**
     * Parses all of the message definitions in a connection header and builds
     * deserializaers for them.
     * @param messages A the message definition header from a {@link Connection}.
     * @throws InvalidDefinitionException If the definition cannot be parsed.
     */
    public void parseMessages(String messages) throws InvalidDefinitionException {
        // The very first message in a definition list does not have its type in
        // front of it, so we need to prepend it.
        messages = "MSG: " + myTopType + "\n" + messages;
        List<String> msgList = MSG_SPLITTER.splitToList(messages);

        ListIterator<String> msgIter = msgList.listIterator(msgList.size());

        // Message definitions from connection metadata print the type of message
        // on that connection first, then the definitions for all of the messages
        // that message references, then all of the messages those reference, and
        // so on.  This means that the simplest message types that do not reference
        // any other complex messages are at the end of the list.  We parse them
        // in reverse order to ensure that we always know the definitions of parsed
        // messages by the time we get to the ones that depend on them.
        myLogger.debug("--- Starting message parsing for " + myTopType + " ---");

        try {
            List<String> unparsedMsgs = parseMessageList(msgIter);
            int unparsedCount = unparsedMsgs.size();

            while (unparsedCount != 0) {
                myLogger.debug("Couldn't parse some messages on the first pass; trying again.");
                unparsedMsgs = parseMessageList(unparsedMsgs.listIterator(unparsedMsgs.size()));
                if (unparsedMsgs.size() == unparsedCount) {
                    myLogger.error(
                            "Unable to parse some messages: " + Joiner.on("====\n").skipNulls().join(unparsedMsgs));
                    throw new InvalidDefinitionException("Unable to parse " + unparsedCount + " messages.");
                }
                else {
                    unparsedCount = unparsedMsgs.size();
                }
            }
        }
        finally {
            myLogger.debug("--- Finished parsing messages ---");
        }
    }

    private List<String> parseMessageList(ListIterator<String> msgIter) throws InvalidDefinitionException {
        List<String> unparseableMessages = Lists.newArrayList();

        while(msgIter.hasPrevious()) {
            String msgStr = msgIter.previous();

            try {
                MessageType msg = new MessageType(msgStr, this);
                myLogger.debug("Constructed deserializer for message type: [" + msg.getPackage() + "]/[" + msg
                        .getType() + "]");

                myMsgTypesByName.put(msg.getType(), msg);
                Map<String, MessageType> pkgMap = myMsgTypesByPackage.get(msg.getPackage());
                if (pkgMap == null) {
                    pkgMap = Maps.newHashMap();
                    myMsgTypesByPackage.put(msg.getPackage(), pkgMap);
                }
                pkgMap.put(msg.getType(), msg);
                myMsgTypeByMd5Sum.put(msg.getMd5Sum(), msg);
            }
            catch (UnknownMessageException e) {
                // If we get this exception, it means the message definition refers
                // to a message we don't know about yet.  It might come later, so
                // store this for now and try again...
                unparseableMessages.add(msgStr);
            }
        }

        return unparseableMessages;
    }

    /**
     * Sets the type of message that is intended to be transmitted over the
     * connection that this collection is associated with.
     * @param type A ROS message type; e. g., "gps_common/GPSStatus"
     */
    public void setTopType(String type) {
        myTopType = type;
    }

    /**
     * Gets a deserializer for the specified package and message.
     * @param pkg A message's package; e. g., "gps_common"
     * @param name A message's type; e. g., "GPSStatus"
     * @return A MessageType that can deserialize that message.
     * @throws UnknownMessageException If no deserializer can be found for that message.
     */
    MessageType getMessageType(String pkg, String name) throws UnknownMessageException {
        Map<String, MessageType> pkgMap = myMsgTypesByPackage.get(pkg);

        if (pkgMap == null) {
            throw new UnknownMessageException(pkg, "");
        }

        MessageType oldMt = pkgMap.get(name);

        if (oldMt == null) {
            throw new UnknownMessageException(pkg, name);
        }

        return oldMt.copy();
    }

    /**
     * Gets a deserializer for the specified message without a package.
     * @param name A message's type; e. g., "Header"
     * @return A MessageType that can deserialize that message.
     * @throws UnknownMessageException If no deserializer can be found for that message.
     */
    MessageType getMessageType(String name) throws UnknownMessageException {
        MessageType oldMt = myMsgTypesByName.get(name);

        if (oldMt == null) {
            throw new UnknownMessageException("[" + name + "]");
        }

        return oldMt.copy();
    }

    /**
     * Gets a deserializer for the message type that was set with
     * {@link #setTopType(String)}.
     * @return A deserializer for the top message type.
     * @throws UnknownMessageException If no deserializer can be found for that message.
     */
    public MessageType getMessageType() throws UnknownMessageException {
        if (myTopType == null) {
            throw new UnknownMessageException("Unknown");
        }

        List<String> types = Splitter.on("/").trimResults().omitEmptyStrings().splitToList(myTopType);

        if (types.size() == 1) {
            return getMessageType(types.get(0));
        }
        else {
            return getMessageType(types.get(0), types.get(1));
        }
    }
}
