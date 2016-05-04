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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Represents a header in a bag file.
 * @see <a href="http://wiki.ros.org/Bags/Format/2.0#Headers">http://wiki.ros.org/Bags/Format/2.0#Headers</a>
 */
public class Header {
    private final Map<String, Field> myFieldMap = Maps.newHashMap();
    private Record.RecordType myType = Record.RecordType.UNKNOWN;

    public Header() {
    }

    /**
     * Reads a header out of a byte buffer.  The entire byte buffer should
     * contain nothing but the header's fields.
     * @param buffer The header's fields.
     * @throws BagReaderException
     */
    public Header(ByteBuffer buffer) throws BagReaderException {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        while (buffer.hasRemaining()) {
            Field field = new Field(buffer);

            myFieldMap.put(field.getName(), field);

            if ("op".equals(field.getName())) {
                switch (field.getFirstByte()) {
                    case 0x03:
                        myType = Record.RecordType.BAG_HEADER;
                        break;
                    case 0x05:
                        myType = Record.RecordType.CHUNK;
                        break;
                    case 0x07:
                        myType = Record.RecordType.CONNECTION;
                        break;
                    case 0x02:
                        myType = Record.RecordType.MESSAGE_DATA;
                        break;
                    case 0x04:
                        myType = Record.RecordType.INDEX_DATA;
                        break;
                    case 0x06:
                        myType = Record.RecordType.CHUNK_INFO;
                        break;
                    default:
                        myType = Record.RecordType.UNKNOWN;
                        break;
                }
            }
        }
    }

    public Record.RecordType getType() {
        return myType;
    }

    /**
     * Returns the value of a particular field in the header as a string.
     * @param fieldName The name of the field to look up.
     * @return The field's value as a string.
     * @throws BagReaderException If that field was not found.
     */
    public String getValue(String fieldName) throws BagReaderException {
        Field field = myFieldMap.get(fieldName);
        checkFields(fieldName, field);
        return field.getString();
    }

    /**
     * Returns the value of a particular field in the header as an int.
     * @param fieldName The name of the field to look up.
     * @return The field's value as a int.
     * @throws BagReaderException If that field was not found.
     */
    public int getInt(String fieldName) throws BagReaderException {
        Field field = myFieldMap.get(fieldName);
        checkFields(fieldName, field);
        return field.getInt();
    }

    /**
     * Returns the value of a particular field in the header as a long.
     * @param fieldName The name of the field to look up.
     * @return The field's value as a long.
     * @throws BagReaderException If that field was not found.
     */
    public long getLong(String fieldName) throws BagReaderException {
        Field field = myFieldMap.get(fieldName);
        checkFields(fieldName, field);
        return field.getLong();
    }

    /**
     * Returns the value of a particular field in the header as a Timestamp.
     * @param fieldName The name of the field to look up.
     * @return The field's value as a Timestamp.
     * @throws BagReaderException If that field was not found.
     */
    public Timestamp getTimestamp(String fieldName) throws BagReaderException {
        Field field = myFieldMap.get(fieldName);
        checkFields(fieldName, field);
        return field.getTimestamp();
    }

    private void checkFields(String fieldName, Field field) throws BagReaderException {
        if (field == null) {
            String fieldstr = Joiner.on(',').join(myFieldMap.keySet());
            throw new BagReaderException("Unknown field: " + fieldName +
                                         "; valid fields are: " + fieldstr);
        }
    }
}
