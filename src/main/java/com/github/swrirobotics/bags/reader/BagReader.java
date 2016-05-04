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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BagReader {
    private static final Logger myLogger = LoggerFactory.getLogger(BagReader.class);

    public static BagFile readFile(File file) throws BagReaderException {
        BagFile bag = new BagFile(file.getAbsolutePath());
        bag.read();
        return bag;
    }

    public static BagFile readFile(String filename) throws BagReaderException {
        BagFile bag = new BagFile(filename);
        bag.read();
        return bag;
    }

    public static void main(String[] args) throws BagReaderException, IOException, UninitializedFieldException {
        myLogger.info("Reading bag files.");
        List<BagFile> bags = new ArrayList<>();
        for (String arg : args) {
            myLogger.info("Reading " + arg);

            try {
                bags.add(readFile(arg));
            }
            catch (BagReaderException e) {
                myLogger.warn("Unable to read bag file: " + arg, e);
            }
        }

        myLogger.info("Successfully read " + bags.size() + " bags.");
        for (BagFile bag : bags) {
            myLogger.info("Path:     " + bag.getPath().toString());

            Double[] gps = bag.getFirstGpsMessage();
            if (gps != null) {
                myLogger.info("Lat/Lon: " + gps[0] + " / " + gps[1]);
            }

            bag.printInfo();
            //bag.analyzeMessageTypes();


            myLogger.info("Md5sum: " + bag.getUniqueIdentifier());

            /*List<Double[]> msgs = bag.getAllGpsMessages().positions;
            myLogger.info(msgs.size() + " GPS messages.");
            for (Double[] msg : msgs) {
                myLogger.info("  Lon/Lat: (" + msg[0] + ", " + msg[1] + ")");
            }*/
            com.github.swrirobotics.bags.reader.messages.serialization.MessageType.printStats();
            //myLogger.info("GPS data:\n" + new String(data.array()));
        }
    }
}
