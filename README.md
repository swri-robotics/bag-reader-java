# Java Bag Reader   [![Build Status](https://travis-ci.org/swri-robotics/bag-reader-java.svg?branch=master)](https://travis-ci.org/swri-robotics/bag-reader-jav)

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
    <version>1.1</version>
</dependency>
```

## Usage

The general pattern you'll follow is:

1. Use the static BagReader.read() methods to obtain a BagFile.
2. Call getConnections() on your BagFile and search through them to find the connections you want based on criteria such as their topic or message type.
3. Create an instance of MsgIterator and use it to iterate through MessageType objects for those connections.

Read over the Javadocs for the BagFile class; it has many other methods you can use to extract data from the bag file in various ways.

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
        String messageType = "std_msgs/String";

        List<Connection> stringConns = Lists.newArrayList();
        for (Connection conn : file.getConnections()) {
            if (conn.getType().equals(messageType)) {
                stringConns.add(conn);
            }
        }

        try (SeekableByteChannel channel = file.getChannel()) {
            for (Connection conn : stringConns) {
                MsgIterator iter = new MsgIterator(file.getChunkInfos(), conn, channel);

                while (iter.hasNext()) {
                    MessageType mt = iter.next();
                    String value = ((StringType) mt.getField("data")).getValue();

                    System.out.println("String from " + conn.getTopic() + ": " + value);
                }
            }
        }
        catch (IOException | UninitializedFieldException e) {
            throw new BagReaderException(e);
        }
    }
}
```
