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

/**
 * Represents information about a topic inside a bag file.
 */
public class TopicInfo implements Comparable<TopicInfo> {
    private final String myName;
    private long myMessageCount = 0;
    private final String myMessageType;
    private final String myMessageMd5Sum;
    private long myConnectionCount = 0;

    /**
     * Creates a new TopicInfo.
     * @param name The name of the topic; e. g., "/localization/gps"
     * @param type The type of the topic; e. g., "std_msgs/GPSFix"
     * @param md5sum The md5 sum of the topic's messages; e. g., "3db3d0a7bc53054c67c528af84710b70"
     */
    public TopicInfo(String name, String type, String md5sum) {
        this.myName = name;
        this.myMessageType = type;
        this.myMessageMd5Sum = md5sum;
    }

    /**
     * The name of the topic; e. g., "/localization/gps"
     * @return The name of the topic.
     */
    public String getName() {
        return myName;
    }

    /**
     * @return The number of messages published on this topic.
     */
    public long getMessageCount() {
        return myMessageCount;
    }

    /**
     * Adds to the number of messages published on this topic.
     * @param count The number to add.
     */
    public void addToMessageCount(long count) {
        myMessageCount += count;
    }

    /**
     * @return The type of message published on this topic.
     */
    public String getMessageType() {
        return myMessageType;
    }

    /**
     * @return The md5sum of the message type published on this topic.
     */
    public String getMessageMd5Sum() {
        return myMessageMd5Sum;
    }

    /**
     * @return The number of connections made on this topic.
     */
    public long getConnectionCount() {
        return myConnectionCount;
    }

    /**
     * Adds one to the number of connections made on this topic.
     */
    public void incrementConnectionCount() {
        myConnectionCount++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicInfo topicInfo = (TopicInfo) o;

        if (myMessageCount != topicInfo.myMessageCount) {
            return false;
        }
        if (myConnectionCount != topicInfo.myConnectionCount) {
            return false;
        }
        if (!myName.equals(topicInfo.myName)) {
            return false;
        }
        return myMessageType.equals(topicInfo.myMessageType);

    }

    @Override
    public int hashCode() {
        int result = myName.hashCode();
        result = 31 * result + (int) (myMessageCount ^ (myMessageCount >>> 32));
        result = 31 * result + myMessageType.hashCode();
        result = 31 * result + myMessageMd5Sum.hashCode();
        result = 31 * result + (int) (myConnectionCount ^ (myConnectionCount >>> 32));
        return result;
    }

    /**
     * Compares the topic's names.
     * Note that for the sake of efficiency, no other fields are compared; within
     * a given bag file, every topic's name should be unique.
     * @param topicInfo The topic to compare to.
     * @return The result of comparing the topics' names together.
     */
    @Override
    public int compareTo(TopicInfo topicInfo) {
        return this.myName.compareTo(topicInfo.myName);
    }
}
