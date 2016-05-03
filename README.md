# Java Bag Reader

This is a Java library intended for reading information from and deserializing [ROS Bag Files](http://wiki.ros.org/Bags).

It is capable of reading [Version 2.0](http://wiki.ros.org/Bags/Format/2.0) bag files and does not require ROS or any other non-Java library to be installed.

The BagReader class is the primary mechanism you'll use to access bag files.  Use it to create a BagFile instance, then use methods on the BagFile to access the data you want.

It does not support writing to or playing back bag files; I would have no objections to adding that functionality, but I haven't needed it for my purposes.

