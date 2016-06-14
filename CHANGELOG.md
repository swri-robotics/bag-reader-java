# Java Bag Reader changelog

1.4

- Added a custom fork of jpountz/lz4-java that is interoperable with the C++ library
- Added the ability to deserialize LZ4-compressed chunks
- Added an API method on BagFile to determine a bag's dominant compression method

1.3

- Implementing bulk deserialization of arrays
- Adding an API call to get a message on a topic at a specific index

1.2

- Removing hard-coded GPS and vehicle name topics
- Adding a better API for iterating through message types and topics

1.1

- Adding examples to the README
- Clarified some error messages
- Made a few API tweaks
- Added lots of Javadocs
- Rearranged package layout to be cleaner
- Supporting Java 1.7

