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

import java.nio.ByteBuffer;

/**
 * Represents a field in a ROS message definition.
 * Everything that can be deserialized is a field.
 */
public interface Field {
    /**
     * Deserializes data from the buffer into this field.  If this field has
     * already read data from the buffer, this will do nothing.
     * @param buffer Bytes that can be deserialized into this field.
     */
    void readMessage(ByteBuffer buffer);

    /**
     * Resets the value read into this field; after calling this,　readMessage
     * can be called again to read another message.
     */
    void reset();

    /**
     * @return The type of data stored in this field.
     */
    String getType();

    /**
     * @return The name of this field if it is contained within another message
     * definition.  May be null if this is a top-level message definition.
     */
    String getName();

    /**
     * Sets the name of this field; implies that it is a member within a message
     * definition.  The top-level message definition for a connection will
     * not have a name.
     * @param name The name of the field.
     */
    void setName(String name);

    /**
     * Creates a duplicate of this field that has the same type and name but
     * represents a different field within a message or a different message.
     * @return The new field.
     */
    Field copy();
}
