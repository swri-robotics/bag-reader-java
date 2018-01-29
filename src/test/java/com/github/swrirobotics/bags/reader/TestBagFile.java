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
import com.github.swrirobotics.bags.reader.records.Connection;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestBagFile {
    @Test
    public void testInt8() throws BagReaderException {
        File file = new File("src/test/resources/Int8.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Int8Type data = message.getField("data");
                try {
                    assertEquals(-127, data.getValue().shortValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt8MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Int8MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                byte[] values = data.getAsBytes();
                assertEquals(-127, values[0]);
                assertEquals(0, values[1]);
                assertEquals(126, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt8() throws BagReaderException {
        File file = new File("src/test/resources/UInt8.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                UInt8Type data = message.getField("data");
                try {
                    assertEquals(180, data.getValue().shortValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt8MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/UInt8MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                short[] values = data.getAsShorts();
                assertEquals(180, values[0]);
                assertEquals(248, values[1]);
                assertEquals(151, values[2]);
                assertEquals(192, values[3]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt16() throws BagReaderException {
        File file = new File("src/test/resources/Int16.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Int16Type data = message.getField("data");
                try {
                    assertEquals(-32767, data.getValue().intValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt16MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Int16MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                short[] values = data.getAsShorts();
                assertEquals(-32767, values[0]);
                assertEquals(0, values[1]);
                assertEquals(32766, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt16() throws BagReaderException {
        File file = new File("src/test/resources/UInt16.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                UInt16Type data = message.getField("data");
                try {
                    assertEquals(65535, data.getValue().intValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt16MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/UInt16MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                int[] values = data.getAsInts();
                assertEquals(0, values[0]);
                assertEquals(30000, values[1]);
                assertEquals(65535, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt32() throws BagReaderException {
        File file = new File("src/test/resources/Int32.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Int32Type data = message.getField("data");
                try {
                    assertEquals(-2147483647, data.getValue().longValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt32MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Int32MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                int[] values = data.getAsInts();
                assertEquals(-2147483647, values[0]);
                assertEquals(0, values[1]);
                assertEquals(2147483646, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt32() throws BagReaderException {
        File file = new File("src/test/resources/UInt32.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                UInt32Type data = message.getField("data");
                try {
                    assertEquals(4294967294L, data.getValue().longValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt32MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/UInt32MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                long[] values = data.getAsLongs();
                assertEquals(0, values[0]);
                assertEquals(2000000000L, values[1]);
                assertEquals(4294967294L, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt64() throws BagReaderException {
        File file = new File("src/test/resources/Int64.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Int64Type data = message.getField("data");
                try {
                    assertEquals(-9223372036854775806L, data.getValue().longValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testInt64MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Int64MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                long[] values = data.getAsLongs();
                assertEquals(-9223372036854775806L, values[0]);
                assertEquals(0L, values[1]);
                assertEquals(9223372036854775806L, values[2]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt64() throws BagReaderException {
        File file = new File("src/test/resources/UInt64.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                UInt64Type data = message.getField("data");
                try {
                    assertEquals(new BigInteger("18446744073709551615"), data.getValue());
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testUInt64MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/UInt64MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                BigInteger[] values = data.getAsBigIntegers();
                assertEquals(BigInteger.ZERO, values[0]);
                assertEquals(new BigInteger("18446744073709551615"), values[1]);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testFloat32() throws BagReaderException {
        File file = new File("src/test/resources/Float32.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Float32Type data = message.getField("data");
                try {
                    assertEquals(3.14159, data.getValue(), 0.00001);
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }


    @Test
    public void testFloat32MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Float32MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                float[] values = data.getAsFloats();
                assertEquals(0.0, values[0], 0.00001);
                assertEquals(3.14159, values[1], 0.00001);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testFloat64() throws BagReaderException {
        File file = new File("src/test/resources/Float64.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                Float64Type data = message.getField("data");
                try {
                    assertEquals(1.003062456558312, data.getValue(), 0.000000001);
                }
                catch (UninitializedFieldException e) {
                    fail();
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }


    @Test
    public void testFloat64MultiArray() throws BagReaderException {
        File file = new File("src/test/resources/Float64MultiArray.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                double[] values = data.getAsDoubles();
                assertEquals(1.003062456558312, values[0], 0.000000001);
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testPointCloud() throws BagReaderException {
        File file = new File("src/test/resources/PointCloud2.bag");
        BagFile bag = new BagFile(file.getPath());
        final int[] count = {0};
        bag.read();
        bag.forMessagesOnTopic("/pointcloud2", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                try {
                    assertEquals(124914,
                                 message.<UInt32Type>getField("width").getValue().longValue());

                    // First, get the array named "fields"
                    ArrayType data = message.getField("fields");
                    List<Field> pointFields = data.getFields();
                    assertEquals(5, pointFields.size());
                    // The array is of type sensor_msgs/PointField, and to read that,
                    // we have to cast it to a MessageType and then access its
                    // individual fields.
                    MessageType pointField = (MessageType)pointFields.get(0);
                    assertEquals("x", pointField.<StringType>getField("name").getValue());
                    assertEquals(0, pointField.<UInt32Type>getField("offset").getValue().intValue());
                    assertEquals(7, pointField.<UInt8Type>getField("datatype").getValue().intValue());
                    assertEquals(1, pointField.<UInt32Type>getField("count").getValue().intValue());

                    pointField = (MessageType)pointFields.get(1);
                    assertEquals("y", pointField.<StringType>getField("name").getValue());

                    pointField = (MessageType)pointFields.get(2);
                    assertEquals("z", pointField.<StringType>getField("name").getValue());

                    pointField = (MessageType)pointFields.get(3);
                    assertEquals("intensity", pointField.<StringType>getField("name").getValue());

                    pointField = (MessageType)pointFields.get(4);
                    assertEquals("ring", pointField.<StringType>getField("name").getValue());
                }
                catch (UninitializedFieldException e) {
                    return false;
                }
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }
}
