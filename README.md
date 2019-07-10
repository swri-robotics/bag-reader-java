# Java Bag Reader   [![Build Status](https://travis-ci.org/swri-robotics/bag-reader-java.svg?branch=master)](https://travis-ci.org/swri-robotics/bag-reader-java)

This is a Java library intended for reading information from and deserializing [ROS Bag Files](http://wiki.ros.org/Bags).

It is capable of reading [Version 2.0](http://wiki.ros.org/Bags/Format/2.0) bag files and does not require ROS or any other non-Java library to be installed.

It does not support writing to or playing back bag files; I would have no objections to adding that functionality, but I haven't needed it for my purposes.

## Requirements

Java 1.7 or higher is required.  Compilation requires Maven 3.0.4 or higher.

## Obtaining

Add the following dependency to your Maven pom.xml:

```xml
<dependency>
    <groupId>com.github.swri-robotics</groupId>
    <artifactId>bag-reader-java</artifactId>
    <version>1.10.2</version>
</dependency>
```

## Usage

The general pattern you'll follow is:

1. Use the static BagReader.read() methods to obtain a BagFile.
2. Call forMessagesOfType() or forMessagesOnTopic() on your BagFile to iterate through all of the messages on a particular type or topic.

Read over the Javadocs for the BagFile class; it has many other methods you can use to extract data from the bag file in various ways, such as looking for only the first message of a type or on a topic, or directly looking through individual connections.

## Type Conversions

The field types in ROS messages do not all have exact equivalents in Java, so they are converted into
similar types that are capable of representing them.  Keep in mind that because Java does not have any unsigned types,
`uint` types must be stored in the next-larger type in order to preserve their entire possible range.

| ROS Type | Java type |
| -------- | --------- |
| bool     | Boolean   |
| byte     | Byte      |
| int8     | Byte      |
| char     | Short     |
| uint8    | Short     |
| int16    | Short     |
| uint16   | Integer   |
| int32    | Integer   |
| uint32   | Long      |
| int64    | Long      |
| uint64   | java.math.BigInteger |
| float32  | Float     |
| float64  | Double    |
| string   | java.lang.String |
| time     | java.sql.Timestamp |
| duration | Double    |

## Examples

### Print info for all of the topics in a bag

```java
public class ExampleClass {
    public static void main(String[] args) throws BagReaderException {
        BagFile file = BagReader.readFile("file.bag");

        System.out.println("Topics:");
        for (TopicInfo topic : file.getTopics()) {
            System.out.println(topic.getName() + " \t\t" + topic.getMessageCount() +
                          " msgs \t: " + topic.getMessageType() + " \t" +
                          (topic.getConnectionCount() > 1 ? ("(" + topic.getConnectionCount() + " connections)") : ""));
        }
    }
}
```

### Print all of the std_msg/String values in a bag

```java
public class ExampleClass {
    public static void main(String[] args) throws BagReaderException {
        BagFile file = BagReader.readFile("file.bag");

        file.forMessagesOfType("std_msgs/String", new MessageHandler() {
            @Override
            public boolean process(MessageType message) {
                try {
                    System.out.println(message.<StringType>getField("data").getValue());
                }
                catch (UninitializedFieldException e) {
                    System.err.println("Field was not initialized.");
                }
                return true;
            }
        });
    }
}
```
