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
import com.github.swrirobotics.bags.reader.exceptions.InvalidDefinitionException;
import com.github.swrirobotics.bags.reader.messages.serialization.MessageCollection;
import org.slf4j.LoggerFactory;

/**
 * Represents a connection in a bag file.
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Connection">http://wiki.ros.org/Bags/Format/2.0#Connection</a>
 */
public class Connection {
    private int myConnectionId;
    private String myTopic;
    private String myType;
    private String myMd5sum;
    private String myMessageDefinition;
    private String myCallerId;
    private Boolean myLatching;
    private final MessageCollection myMsgCollection = new MessageCollection();

    public Connection(Record record) throws BagReaderException {
        myConnectionId = record.getHeader().getInt("conn");
        myTopic = record.getHeader().getValue("topic");
        myType = record.getConnectionHeader().getValue("type");
        myMd5sum = record.getConnectionHeader().getValue("md5sum");
        myMessageDefinition = record.getConnectionHeader().getValue("message_definition");
        try {
            myMsgCollection.setTopType(myType);
            myMsgCollection.parseMessages(myMessageDefinition);
        }
        catch (InvalidDefinitionException e) {
            LoggerFactory.getLogger(Connection.class).error("Error configuring message deserializer: ", e);
        }

        // callerId and latching are optional
        try {
            myCallerId = record.getConnectionHeader().getValue("callerid");
        }
        catch (BagReaderException e) {
            myCallerId = null;
        }
        try {
            myLatching = Boolean.parseBoolean(record.getConnectionHeader().getValue("latching"));
        }
        catch (BagReaderException e) {
            myLatching = null;
        }
    }

    public int getConnectionId() {
        return myConnectionId;
    }

    public String getTopic() {
        return myTopic;
    }

    public String getType() {
        return myType;
    }

    public String getMd5sum() {
        return myMd5sum;
    }

    public String getMessageDefinition() {
        return myMessageDefinition;
    }

    public String getCallerId() {
        return myCallerId;
    }

    public Boolean getLatching() {
        return myLatching;
    }

    /**
     * Gets a {@link MessageCollection} object that is capable of deserializing
     * the messages sent on this connection.
     * @return A MessageCollection that can deserialize this connection's messages.
     */
    public MessageCollection getMessageCollection() {
        return myMsgCollection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Connection that = (Connection) o;

        if (myConnectionId != that.myConnectionId) {
            return false;
        }
        if (!myTopic.equals(that.myTopic)) {
            return false;
        }
        if (!myType.equals(that.myType)) {
            return false;
        }
        if (!myMd5sum.equals(that.myMd5sum)) {
            return false;
        }
        if (!myMessageDefinition.equals(that.myMessageDefinition)) {
            return false;
        }
        if (myCallerId != null ? !myCallerId.equals(that.myCallerId) : that.myCallerId != null) {
            return false;
        }
        return !(myLatching != null ? !myLatching.equals(that.myLatching) : that.myLatching != null);

    }

    @Override
    public int hashCode() {
        int result = myConnectionId;
        result = 31 * result + myTopic.hashCode();
        result = 31 * result + myType.hashCode();
        result = 31 * result + myMd5sum.hashCode();
        result = 31 * result + myMessageDefinition.hashCode();
        result = 31 * result + (myCallerId != null ? myCallerId.hashCode() : 0);
        result = 31 * result + (myLatching != null ? myLatching.hashCode() : 0);
        return result;
    }
}
