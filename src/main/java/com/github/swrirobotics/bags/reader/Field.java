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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Represents a field inside a header in a bag file.
 *
 * Note that all of the getter functions for this class do no error checking.
 * You'll probably get an exception if you try to call one and the field
 * doesn't actually have enough bytes in it.
 *
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Headers">http://wiki.ros.org/Bags/Format/2.0#Headers</a>
 */
public class Field {
    private final int myLength;
    private String myName;
    private byte[] myValue;

    private boolean myGotInt = false;
    private int myIntValue = 0;
    private boolean myGotLong = false;
    private long myLongValue = 0;
    private String myStringValue = null;
    private Timestamp myTimestampValue = null;

    /**
     * Creates a new field by reading it from the byte buffer.
     * @param buffer The buffer to read the field from.
     * @throws BagReaderException
     */
    public Field(ByteBuffer buffer) throws BagReaderException {
        myLength = buffer.getInt();
        if (myLength > 100000L) {
            throw new BagReaderException("Header field is unreasonably large (" +
                                         myLength +
                                         " bytes).  Bag file may need to be reindexed.");
        }
        else {
            byte fieldData[] = new byte[myLength];
            buffer.get(fieldData);
            fieldData = ByteBuffer.wrap(fieldData).order(ByteOrder.LITTLE_ENDIAN).array();

            int separatorPos = -1;
            for (int i = 0; i < fieldData.length; i++) {
                if (fieldData[i] == '=') {
                    separatorPos = i;
                    break;
                }
            }

            if (separatorPos > -1) {
                myName = new String(Arrays.copyOfRange(fieldData, 0, separatorPos));
                myValue = Arrays.copyOfRange(fieldData, separatorPos + 1, fieldData.length);
            }
            else {
                throw new BagReaderException("Unable to find separator in header.");
            }
        }
    }

    /**
     * @return The length of the field's name and value combined.
     */
    public int getLength() {
        return myLength;
    }

    /**
     * @return The name of the field.
     */
    public String getName() {
        return myName;
    }

    /**
     * @return The value of the field as an integer.
     */
    public int getInt() {
        if (!myGotInt) {
            myIntValue = ByteBuffer.wrap(myValue).order(ByteOrder.LITTLE_ENDIAN).getInt();
            myGotInt = true;
        }
        return myIntValue;
    }

    /**
     * @return The value of the field as a long.
     */
    public long getLong() {
        if (!myGotLong) {
            myLongValue = ByteBuffer.wrap(myValue).order(ByteOrder.LITTLE_ENDIAN).getLong();
            myGotLong = true;
        }
        return myLongValue;
    }

    /**
     * @return Just the first byte of the field's value.
     */
    public byte getFirstByte() {
        return myValue[0];
    }

    /**
     * @return The value of the field as a string.
     */
    public String getString() {
        if (myStringValue == null) {
            myStringValue = new String(myValue);
        }
        return myStringValue;
    }

    /**
     * @return The value of the field as a timestamp.
     */
    public Timestamp getTimestamp() {
        if (myTimestampValue == null) {
            ByteBuffer buffer = ByteBuffer.wrap(myValue).order(ByteOrder.LITTLE_ENDIAN);
            long secs = (long) buffer.getInt();
            int nsecs = buffer.getInt();
            myTimestampValue = new Timestamp(secs * 1000L);
            myTimestampValue.setNanos(nsecs);
        }
        return myTimestampValue;
    }
}
