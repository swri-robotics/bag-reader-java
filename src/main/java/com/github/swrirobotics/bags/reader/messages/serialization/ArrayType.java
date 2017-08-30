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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
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

    private ByteBuffer myData = null;

    public ArrayType(Field field, int length) {
        myBaseType = field;
        myLength = length;
        myTypeName = myBaseType.getType() + "[" + (myLength > 0 ? myLength : "") + "]";
    }

    /**
     * Used to access the individual fields in the array after
     * {@link #readMessage(ByteBuffer)} has been called.  There will
     * be one Field object for every value in the array.  Only call this for
     * strings and complex message types; primitive types should be accessed
     * through one of the other "get" methods.
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

    public void setOrder(ByteOrder order) {
        myData = myData.order(order);
    }

    /**
     * Returns the contents of the buffer as bytes.  This is appropriate for
     * byte and int8 types.  Note that "byte" is deprecated.
     */
    public byte[] getAsBytes() {
        byte[] tmp = new byte[myData.capacity()];
        myData.get(tmp);
        return tmp;
    }

    /**
     * Returns the contents of the buffer as shorts.  This is appropriate for
     * char, uint8, and int16 types.  Note that "char" is deprecated.
     */
    public short[] getAsShorts() {
        short tmp[];
        if (myTypeName.equals("uint8[]")) {
            tmp = new short[myData.capacity()];
            byte bytes[] = new byte[myData.capacity()];
            myData.get(bytes);
            for(int i = 0; i < bytes.length; i++) {
                tmp[i] = (short)((short)bytes[i] & 0xff);
            }
        }
        else {
            tmp = new short[myData.capacity() / 2];
            myData.asShortBuffer().get(tmp);
        }
        return tmp;
    }

    /**
     * Returns the contents of the buffer as ints.  This is appropriate for
     * uint16 and int32 types.
     */
    public int[] getAsInts() {
        int[] tmp;
        if (myTypeName.equals("uint16[]")) {
            tmp = new int[myData.capacity() / 2];
            short shorts[] = new short[myData.capacity() / 2];
            myData.asShortBuffer().get(shorts);
            for (int i = 0; i < shorts.length; i++) {
                tmp[i] = (int)shorts[i] & 0xffff;
            }
        }
        else {
            tmp = new int[myData.capacity() / 4];
            myData.asIntBuffer().get(tmp);
        }
        return tmp;
    }

    /**
     * Returns the contents of the buffer as longs.  This is appropriate for
     * uint32 and int64 types.
     */
    public long[] getAsLongs() {
        long[] tmp;
        if (myTypeName.equals("uint32[]")) {
            tmp = new long[myData.capacity() / 4];
            int ints[] = new int[myData.capacity() / 4];
            myData.asIntBuffer().get(ints);
            for (int i = 0; i < ints.length; i++) {
                tmp[i] = (long)ints[i] & 0xffffffffL;
            }
        }
        else {
            tmp = new long[myData.capacity() / 8];
            myData.asLongBuffer().get(tmp);
        }
        return tmp;
    }

    /**
     * Returns the contents of the buffer as BigIntegers.  This is appropriate
     * for uint64 types.
     * @return
     */
    public BigInteger[] getAsBigIntegers() {
        BigInteger[] bigints = new BigInteger[myData.capacity() / 8];
        byte[] tmp = new byte[8];
        for (int i = 0; i < myData.capacity(); i += 8) {
            myData.get(tmp);
            bigints[i / 8] = new BigInteger(1, tmp);
        }
        return bigints;
    }

    /**
     * Returns the contents of the buffer as floats.  This is appropriate
     * for the float32 type.
     */
    public float[] getAsFloats() {
        float[] tmp = new float[myData.capacity() / 4];
        myData.asFloatBuffer().get(tmp);
        return tmp;
    }

    /**
     * Returns the contents of the buffer as doubles.  This is appropriate
     * for the float64 type.
     */
    public double[] getAsDoubles() {
        double[] tmp = new double[myData.capacity() / 8];
        myData.asDoubleBuffer().get(tmp);
        return tmp;
    }

    /**
     * Returns the contents of the buffer as doubles that represent a
     * duration in seconds.  This is appropriate for the duration type.
     */
    public double[] getAsDurations() {
        int[] intValues = getAsInts();
        double[] durations = new double[intValues.length/2];
        for (int i = 0; i < intValues.length; i += 2) {
            int secs = intValues[0];
            int nsecs = intValues[1];
            durations[i/2] = (double)secs + ((double)nsecs) / 1000000000.0;
        }

        return durations;
    }

    /**
     * Returns the contents of the buffer as Timestamps.  This is appropriate
     * for the time type.
     */
    public Timestamp[] getAsTimestamps() {
        int[] intValues = getAsInts();
        Timestamp[] timestamps = new Timestamp[intValues.length/2];
        for (int i = 0; i < intValues.length; i += 2) {
            int rawSecs = intValues[0];
            int rawNsecs = intValues[1];

            long secs = rawSecs >= 0 ? rawSecs : 0x100000000L + rawSecs;
            long nsecs = rawNsecs >= 0 ? rawNsecs : 0x100000000L + rawNsecs;

            Timestamp val = new Timestamp(secs * 1000);
            val.setNanos((int) nsecs);
            timestamps[i/2] = val;
        }

        return timestamps;
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

        // For most types of data, we know how large they're going to be and
        // can go ahead and read all of that in and store it in a ByteBuffer;
        // we can delay deserializing it until it's actually accessed.
        // For strings and complex message types, the individual items in
        // the array are variable-length, and we don't know how long the next
        // one will be until we've examined the first, so we might as well
        // go ahead and deserialize all of them.

        byte[] tempBytes;
        switch (myBaseType.getType()) {
            case "bool":
            case "byte":
            case "char":
            case "int8":
            case "uint8":
                tempBytes = new byte[length];
                buffer.get(tempBytes);
                myData = ByteBuffer.wrap(tempBytes);
                break;
            case "int16":
            case "uint16":
                tempBytes = new byte[length * 2];
                buffer.get(tempBytes);
                myData = ByteBuffer.wrap(tempBytes).order(ByteOrder.LITTLE_ENDIAN);
                break;
            case "int32":
            case "uint32":
            case "float32":
                tempBytes = new byte[length * 4];
                buffer.get(tempBytes);
                myData = ByteBuffer.wrap(tempBytes).order(ByteOrder.LITTLE_ENDIAN);
                break;
            case "duration":
            case "float64":
            case "int64":
            case "time":
            case "uint64":
                tempBytes = new byte[length * 8];
                buffer.get(tempBytes);
                myData = ByteBuffer.wrap(tempBytes).order(ByteOrder.LITTLE_ENDIAN);
                break;
            default:
                // strings and other complex messages
                for(int i = 0; i < length; i++) {
                    Field instance = myBaseType.copy();
                    instance.readMessage(buffer);
                    myFields.add(instance);
                }
                break;
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
