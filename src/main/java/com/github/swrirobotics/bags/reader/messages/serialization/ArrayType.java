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

import com.github.swrirobotics.bags.reader.exceptions.UninitializedFieldException;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Represents a field in a ROS message that contains an array of values.
 */
public class ArrayType implements Field {
    private final Field myBaseType;
    private final String myTypeName;
    private final int myLength;
    private final List<Field> myFields = Lists.newArrayList();
    private String myName;

    public ArrayType(Field field, int length) {
        myBaseType = field;
        myLength = length;
        myTypeName = myBaseType.getType() + "[" + (myLength > 0 ? myLength : "") + "]";
    }

    /**
     * Used to access the individual fields in the array after
     * {@link #readMessage(ByteBuffer)} has been called.  There will
     * be one Field object for every value in the array.
     * @return All of the fields that have been read.
     * @throws UninitializedFieldException If the object has not yet read in data
     *                                     or if a variable-length array had a length of 0.
     */
    public List<Field> getFields() throws UninitializedFieldException {
        if (myFields.isEmpty()) {
            throw new UninitializedFieldException();
        }

        return myFields;
    }

    @Override
    public void readMessage(ByteBuffer buffer) {
        if (!myFields.isEmpty()) {
            return;
        }

        int length = myLength;
        if (length == 0) {
            length = buffer.getInt();
        }

        for(int i = 0; i < length; i++) {
            // TODO This is probably very slow; it could be optimized by
            // implementing it individually for each primitive type and
            // using bulk reads.
            Field instance = myBaseType.copy();
            instance.readMessage(buffer);
            myFields.add(instance);
        }
    }

    @Override
    public void reset() {
        myFields.clear();
    }

    @Override
    public String getType() {
        return myTypeName;
    }

    @Override
    public String getName() {
        return myName;
    }

    @Override
    public Field copy() {
        ArrayType newAt = new ArrayType(myBaseType.copy(), myLength);
        newAt.setName(myName);

        return newAt;
    }

    @Override
    public void setName(String name) {
        myName = name;
    }
}
