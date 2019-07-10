# Java Bag Reader changelog

1.10.2

- Upgrade dependencies with security vulnerabilities

1.10.1

- Upgrade dependencies with security vulnerabilities

1.10.0

- Improve memory usage for bag with lots of messages
- Fix NullPointerException when comparing message indexes
- Fix ArrayIndexOutOfBoundError when getting messages at certain indexes

1.9.0

- Fix reading arrays of primitive types
- Add MessageType.getFieldNames() method

1.8

- Fix bugs when reading LZ4-compressed bags

1.7

- Also check times in index data records when determing start/end time

1.6

- Removed file locking when reading bag files

1.5

- Acquiring shared locks when reading bag files
- Logging warnings and throwing an exception if it fails to read the expected number of chunks and connections

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

