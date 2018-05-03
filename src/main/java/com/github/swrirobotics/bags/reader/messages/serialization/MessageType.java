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

import com.github.swrirobotics.bags.reader.exceptions.InvalidDefinitionException;
import com.github.swrirobotics.bags.reader.exceptions.UnknownMessageException;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and deserializes complex ROS message types.
 *
 * @see <a href="http://wiki.ros.org/msg">http://wiki.ros.org/msg</a>
 */
public class MessageType implements Field {
    private String myPackage;
    private String myType;
    private String myMd5Sum;
    private String myName = null;

    private MessageCollection myMsgCollection;

    /**
     * Set this static boolean true to collect de-serialization statistics
     * (will use a lot of memory for a file with many messages)
     */
    private final static boolean COLLECT_STATS = false;
    private static final Map<String, Map<String, List<Long>>> STATS = Maps.newHashMap();

    private final List<Field> myFields = Lists.newArrayList();
    private final Map<String, Field> myFieldNameMap = Maps.newHashMap();

    private static final Logger myLogger = LoggerFactory.getLogger(MessageType.class);

    private static final Splitter LINE_SPLITTER = Splitter.on('\n').trimResults().omitEmptyStrings();
    private static final Splitter NAME_SPLITTER = Splitter.on('/').trimResults().omitEmptyStrings();

    private static final Pattern FIELD_PATTERN = Pattern.compile("^\\s*([\\w/\\[\\]]+)\\s+([\\w]+)\\s*(?:=\\s*([^\\s]+))?.*#?.*$");
    private static final Pattern ARRAY_PATTERN = Pattern.compile("^([\\w/]+)\\[(\\d*)\\]$");
    private static final Pattern STRING_CONST_PATTERN = Pattern.compile("^\\s*string\\s+\\w+\\s*=.*$");
    private static final Pattern CONST_PATTERN = Pattern.compile("^\\w+=.+$");
    private static final Pattern PRIMITIVE_PATTERN = Pattern.compile("^((bool)|(char)|(byte)|(u?int((8)|(16)|(32)|(64)))|(float((32)|(64)))|(string)|(time)|(duration)).*$");
    private static final Pattern TYPE_PATTERN = Pattern.compile("^(([\\w/]+)(?:\\s*\\[\\s*\\d*\\s*\\])?).*$");

    /**
     * Parses a message definition to create a new deserializer.
     *
     * @param definition    The definition of a single ROS message.
     * @param msgCollection A collection of other messages that this message
     *                      may depend on.
     * @throws InvalidDefinitionException If the definition cannot be parsed.
     * @throws UnknownMessageException    If this message definition refers to
     *                                    another message that is not in the message collection.
     */
    public MessageType(String definition, MessageCollection msgCollection) throws InvalidDefinitionException, UnknownMessageException {
        myMsgCollection = msgCollection;

        Iterable<String> lines = LINE_SPLITTER.split(definition);

        Iterator<String> iter = lines.iterator();
        if (!iter.hasNext()) {
            throw new InvalidDefinitionException("Message definition had no lines.");
        }

        // Figure out the type of the message first; the first line should look like:
        // MSG: std_msgs/Header
        String typeStr = iter.next();
        if (!typeStr.startsWith("MSG: ")) {
            throw new InvalidDefinitionException("Message definition did not start with \"MSG: \":\n" + typeStr);
        }
        typeStr = typeStr.replace("MSG: ", "");

        List<String> type = NAME_SPLITTER.splitToList(typeStr);
        if (type.size() != 2) {
            throw new InvalidDefinitionException("Unable to parse message type: \"" + typeStr + "\"");
        }
        myPackage = type.get(0);
        myType = type.get(1);
        myLogger.debug("Parsing message type: " + myPackage + "/" + myType);

        // Now we can get all of the fields.
        while (iter.hasNext()) {
            String line = iter.next().trim();

            if (line.isEmpty() || line.startsWith("#")) {
                // Skip empty lines and comments
                continue;
            }

            Matcher fieldMatcher = FIELD_PATTERN.matcher(line);
            if (!fieldMatcher.matches()) {
                throw new InvalidDefinitionException("Unable to parse field definition: \n" + line);
            }

            String fieldType = fieldMatcher.group(1);
            String fieldName = fieldMatcher.group(2);
            String defaultVal = null;
            if (fieldMatcher.groupCount() == 3) {
                defaultVal = fieldMatcher.group(3);
            }

            Field field = createField(fieldType, fieldName, defaultVal);
            myFields.add(field);
            myFieldNameMap.put(field.getName(), field);
        }

        try {
            myMd5Sum = calculateMd5Sum(definition);
        }
        catch (UnknownMessageException e) {
            myLogger.error("Error calculating md5sum", e);
        }
        myLogger.debug("MD5Sum: " + myMd5Sum);
    }

    /**
     * Creating a MessageType without a definition isn't allowed.
     */
    private MessageType() {

    }

    /**
     * Calculates the MD5 Sum for a message definition.  This should be identical
     * to the MD5 Sum calculated by the Python roslib tools.
     * <p>
     * The process used to calculate it is described at
     * {@see <a href="http://wiki.ros.org/ROS/Technical%20Overview#Message_serialization_and_msg_MD5_sums">http://wiki.ros.org/ROS/Technical%20Overview#Message_serialization_and_msg_MD5_sums</a>}
     * but... the description is not quite correct! Notably:
     * 1) Only leading and trailing whitespace are removed.  Whitespace within
     * a field definition is left intact.
     * 2) Non-primitive message types are replaced with the md5sum of that
     * message type.
     *
     * @param definition A ROS message definition.
     * @return That message's MD5 hash.
     * @throws UnknownMessageException If the message refers to another message
     *                                 type that we don't know about.
     */
    private String calculateMd5Sum(String definition) throws UnknownMessageException {
        List<String> rawLines = LINE_SPLITTER.splitToList(definition);
        List<String> constants = Lists.newArrayList();
        List<String> nonconstants = Lists.newArrayList();

        for (String line : rawLines) {
            // Start by removing leading & trailing whitespace
            line = line.trim();
            if (line.startsWith("MSG:")) {
                // Ignore the MSG: definition line added to connection headers
                continue;
            }
            if (!STRING_CONST_PATTERN.matcher(line).matches()) {
                // Remove comments for anything other than string constants
                line = line.replaceAll("\\s*#.*$", "");
            }

            if (line.isEmpty()) {
                // Ignore empty lines
                continue;
            }

            if (!PRIMITIVE_PATTERN.matcher(line).matches()) {
                // Here's the complex part.  For any non-primitive types, their
                // type declaration is removed and replaced with the md5sum of
                // that type.  That includes arrays; for example, all of these
                // lines:
                // Header h1
                // std_msgs/Header h2
                // Header[] headers
                // Would be replaced like so:
                // 2176decaecbce78abc3b96ef049fabed h1
                // 2176decaecbce78abc3b96ef049fabed h2
                // 2176decaecbce78abc3b96ef049fabed headers

                // If we got this far, this isn't a primitive type.  Match the
                // type at the beginning of the line...
                Matcher typeMatcher = TYPE_PATTERN.matcher(line);
                if (typeMatcher.matches()) {
                    // Group 2 is only the type, no array brackets
                    String type = typeMatcher.group(2);
                    List<String> typeParts = NAME_SPLITTER.splitToList(type);
                    MessageType mt;
                    // Try to look up the message via the package if we can,
                    // but since it's possible to specific messages without
                    // the package, we might have to just use the name.
                    if (typeParts.size() == 1) {
                        mt = myMsgCollection.getMessageType(typeParts.get(0));
                    }
                    else {
                        mt = myMsgCollection.getMessageType(typeParts.get(0), typeParts.get(1));
                    }
                    // Group 2 includes the array declaration, which should
                    // also be replaced.
                    line = line.replace(typeMatcher.group(1), mt.getMd5Sum());
                }
            }

            if (CONST_PATTERN.matcher(line).matches()) {
                // Reorder constants so they come first
                constants.add(line);
            }
            else {
                nonconstants.add(line);
            }
        }

        List<String> filteredLines = Lists.newArrayList();
        filteredLines.addAll(constants);
        filteredLines.addAll(nonconstants);
        String filteredText = Joiner.on("\n").skipNulls().join(filteredLines);

        try {
            byte[] md5sum = MessageDigest.getInstance("MD5").digest(filteredText.getBytes());
            StringBuilder builder = new StringBuilder();
            for (byte b : md5sum) {
                builder.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
            }

            return builder.toString();
        }
        catch (NoSuchAlgorithmException e) {
            myLogger.error("Unable to get MD5 algorithm.");
        }

        return "";
    }

    /**
     * Makes a deep copy of this message.  You should make a copy of a
     * MessageType every time you deserialize a new message in order to
     * ensure every individual deserializer is in a clean state.
     * This copies the MessageType's package, type, md5sum, name,
     * and all of its fields.
     *
     * @return A deep copy of this MessageType.
     */
    public MessageType copy() {
        MessageType mt = new MessageType();
        mt.myPackage = myPackage;
        mt.myType = myType;
        mt.myMd5Sum = myMd5Sum;
        mt.myMsgCollection = myMsgCollection;
        mt.myName = myName;

        for (Field field : myFields) {
            Field newField = field.copy();
            mt.myFields.add(newField);
            mt.myFieldNameMap.put(newField.getName(), newField);
        }

        return mt;
    }

    /**
     * Deserializes a message from a byte buffer.
     *
     * @param buffer Bytes that can be deserialized into this field.
     */
    @Override
    public void readMessage(ByteBuffer buffer) {
        long startTimeNs = 0;
        if (COLLECT_STATS) {
            startTimeNs = System.nanoTime();
        }
        for (Field field : myFields) {
            field.readMessage(buffer);
        }
        long finishTimeNs = System.nanoTime();
        if (COLLECT_STATS) {
            Map<String, List<Long>> msgTimeStats = STATS.get(myPackage);
            if (msgTimeStats == null) {
                msgTimeStats = Maps.newHashMap();
                STATS.put(myPackage, msgTimeStats);
            }
            List<Long> timeStats = msgTimeStats.get(myType);
            if (timeStats == null) {
                timeStats = Lists.newArrayList();
                msgTimeStats.put(myType, timeStats);
            }
            timeStats.add(finishTimeNs - startTimeNs);
        }
    }

    /**
     * Clears all of the values stored in this message's fields from
     * a previous deserialization.
     */
    @Override
    public void reset() {
        for (Field field : myFields) {
            field.reset();
        }
    }

    /**
     * Creates a new deserializer for a field in this message..
     *
     * @param type       The type of field to create; e. g., "gps_common/GPSStatus" or "uint32"
     * @param name       The name of the field, if it is a member within a message.
     *                   May be null for a top-level message.
     * @param defaultVal The value of the field if it is a constant expression.
     *                   Set this to null for non-constant fields.
     * @return A deserializer for a field.
     * @throws UnknownMessageException If the field's type does not match any known messages.
     */
    private Field createField(String type, String name, String defaultVal) throws UnknownMessageException {
        Matcher m = ARRAY_PATTERN.matcher(type);
        boolean isArray = m.matches();
        int arraySize = 0;

        String baseType;
        if (isArray) {
            baseType = m.group(1);
            if (!m.group(2).isEmpty()) {
                arraySize = Integer.valueOf(m.group(2));
            }
        }
        else {
            baseType = type;
        }

        Field field;
        switch (baseType) {
            case "bool":
                field = new BoolType();
                break;
            case "int8":
            case "byte":
                field = new Int8Type();
                break;
            case "uint8":
            case "char":
                field = new UInt8Type();
                break;
            case "int16":
                field = new Int16Type();
                break;
            case "uint16":
                field = new UInt16Type();
                break;
            case "int32":
                field = new Int32Type();
                break;
            case "uint32":
                field = new UInt32Type();
                break;
            case "int64":
                field = new Int64Type();
                break;
            case "uint64":
                field = new UInt64Type();
                break;
            case "float32":
                field = new Float32Type();
                break;
            case "float64":
                field = new Float64Type();
                break;
            case "string":
                field = new StringType();
                break;
            case "time":
                field = new TimeType();
                break;
            case "duration":
                field = new DurationType();
                break;
            default:
                List<String> nameParts = NAME_SPLITTER.splitToList(baseType);
                if (nameParts.size() == 1) {
                    field = myMsgCollection.getMessageType(baseType);
                }
                else {
                    field = myMsgCollection.getMessageType(nameParts.get(0), nameParts.get(1));
                }
                break;
        }

        if (isArray) {
            field = new ArrayType(field, arraySize);
        }
        field.setName(name);

        if (defaultVal != null && field instanceof PrimitiveType) {
            ((PrimitiveType) field).setDefaultValue(defaultVal);
        }

        return field;
    }

    /**
     * Returns a list containing all of the field names found in this message.
     *
     * @return
     */
    public List<String> getFieldNames() {
        return Lists.newArrayList(myFieldNameMap.keySet());
    }

    /**
     * The ROS message type without a package; for example, "String"
     *
     * @return The ROS message type of the field.
     */
    @Override
    public String getType() {
        return myType;
    }

    /**
     * The ROS package containing the message definition; for example, "std_msgs"
     *
     * @return The ROS package containing this message definition.
     */
    public String getPackage() {
        return myPackage;
    }

    /**
     * The MD5 sum of the ROS message definition; for example, "992ce8a1687cec8c8bd883ec73ca41d1"
     *
     * @return The md5 sum of the ROS message definition.
     */
    public String getMd5Sum() {
        return myMd5Sum;
    }

    /**
     * If this is a field within another ROS message, this will be the
     * name of the field. For example, this will be "status" if this
     * object represents the sensor_msgs/NavSatStatus field that is inside
     * a sensor_msgs/NavSatFix message.
     * If this object represents a top-level message and not an inner
     * field, it will be null.
     *
     * @return The name of this field if it is inside another message
     * definition or null if it is not.
     */
    @Override
    public String getName() {
        return myName;
    }

    @Override
    public void setName(String name) {
        myName = name;
    }

    /**
     * Gets the value of a field with the given name within this message.
     * <p>
     * This will be an instance of one of the classes that implements {@link Field}.
     * It could be one of the following:
     * <ul>
     * <li>It could be another {@link MessageType}, in which case you can
     * call {@link #getField(String)} to get its fields.</li>
     * <li>It could be an instance of {@link ArrayType}, in which case you can call
     * {@link ArrayType#getFields()} to get the individual fields that represent
     * its data.</li>
     * <li>It could be an instance of any of the classes that implement {@link PrimitiveType},
     * in which case you can cast it and call {@link PrimitiveType#getValue()} to get
     * the underlying data.</li>
     * </ul>
     *
     * @param <T>  The type of field that you are expecting.
     * @param name The name of the field to retrieve.
     * @return An object representing that field.
     * @throws ClassCastException If the set parameter type is not the same as the actual
     *                            field type.
     */
    @SuppressWarnings("unchecked")
    public <T extends Field> T getField(String name) {
        return (T) myFieldNameMap.get(name);
    }

    /**
     * In order to analyze deserialization performance, the
     * {@link #readMessage(ByteBuffer)} method collects information about how
     * long it takes to deserialize different message types.  This method will
     * print a list of all of the different types of messages that have been
     * read, how many have been read, and the average time to deserialize them.
     * To use printStats, set COLLECT_STATS to true,
     * since collecting statistics uses a lot memory that cannot be garbage collected.
     */
    public static void printStats() {
        if (!COLLECT_STATS) {
            myLogger.warn("set MessageType.COLLECT_STATS=true to collect timing statistics");
            return;
        }
        myLogger.info("--- Message decoding statistics ---");
        for (Map.Entry<String, Map<String, List<Long>>> pkg : STATS.entrySet()) {
            for (Map.Entry<String, List<Long>> type : pkg.getValue().entrySet()) {
                double avgTime = 0;
                for (Long time : type.getValue()) {
                    avgTime += time;
                }
                avgTime /= (double) type.getValue().size();
                myLogger.info("  Type: " + pkg.getKey() + "/" + type.getKey() +
                                      " : " + type.getValue().size() +
                                      " msgs, averaged " + avgTime +
                                      " ns per msg.");
            }
        }
    }
}
