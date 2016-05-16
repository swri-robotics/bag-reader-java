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
import com.github.swrirobotics.bags.reader.messages.serialization.Float64Type;
import com.github.swrirobotics.bags.reader.messages.serialization.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides methods for reading files and producing {@link BagFile}s.
 * <p>
 *     Also provides a {@link #main(String[])} method that can be used as an
 *     entry point for functionality similar to the "rosbag info" command.
 * </p>
 */
public class BagReader {
    private static final Logger myLogger = LoggerFactory.getLogger(BagReader.class);

    /**
     * Reads a V2.0 ROS bag file and returns a {@link BagFile} that represents it.
     * @param file The file to read.
     * @return An object that can be used to extract information from the bag.
     * @throws BagReaderException If there was an error reading or parsing the file.
     */
    public static BagFile readFile(File file) throws BagReaderException {
        BagFile bag = new BagFile(file.getAbsolutePath());
        bag.read();
        return bag;
    }

    /**
     * Reads a V2.0 ROS bag file and returns a {@link BagFile} that represents it.
     * @param filename The path of the file to read.
     * @return An object that can be used to extract information from the bag.
     * @throws BagReaderException If there was an error reading or parsing the file.
     */
    public static BagFile readFile(String filename) throws BagReaderException {
        BagFile bag = new BagFile(filename);
        bag.read();
        return bag;
    }

    /**
     * Reads in a series of bag files from the command line and prints
     * information about them; this is similar to the output of
     * "rosbag info".
     * @param args A list of filenames to read.
     * @throws BagReaderException If there were errors parsing the bag files.
     */
    public static void main(String[] args) throws BagReaderException, UninitializedFieldException {
        // TODO It would be nice if there were some command line arguments
        // you could use here to extract different types of information
        // and make this more useful as a command-line utility.
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

            // Prints out a block of info similar to "rosbag info".
            bag.printInfo();

            // Gets the first GPS message and prints it.
            MessageType msg = bag.getFirstMessageOfType("gps_common/GPSFix");
            if (msg == null) {
                msg = bag.getFirstMessageOfType("sensor_msgs/NavSatFix");
            }
            if (msg == null) {
                msg = bag.getFirstMessageOfType("marti_gps_common/GPSFix");
            }
            if (msg != null) {
                myLogger.info("Lat/Lon: " +
                              msg.<Float64Type>getField("latitude").getValue() + " / " +
                              msg.<Float64Type>getField("longitude").getValue());
            }

            // Prints out a unique fingerprint for the bag file.  Note that
            // this is not the same as doing an md5sum of the entire file.
            myLogger.info("Bag fingerprint: " + bag.getUniqueIdentifier());

            // Prints some statistics about message deserialization.
            //MessageType.printStats();
        }
    }
}
