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
import com.github.swrirobotics.bags.reader.messages.serialization.ArrayType;
import com.github.swrirobotics.bags.reader.messages.serialization.Float64Type;
import com.github.swrirobotics.bags.reader.messages.serialization.MessageType;
import com.github.swrirobotics.bags.reader.messages.serialization.UInt8Type;
import com.github.swrirobotics.bags.reader.records.Connection;
import com.google.common.base.Joiner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestBagFile {
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
                    assertEquals(180, data.getValue().longValue());
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
        final Logger logger = LoggerFactory.getLogger(TestBagFile.class);
        bag.forMessagesOnTopic("/data", new MessageHandler() {
            @Override
            public boolean process(MessageType message, Connection connection) {
                ArrayType data = message.getField("data");
                logger.info("Type: " + data.getType());
                short[] values = data.getAsShorts();
                logger.info("Values: " + values[0] + " " + values[1] + " " + values[2] + " " + values[3]);
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
}
