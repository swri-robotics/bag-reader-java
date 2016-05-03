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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Implements a SeekableByteChannel that can be used to read from
 * a ByteBuffer.
 */
public class ByteBufferChannel implements SeekableByteChannel {
    private final ByteBuffer myBuffer;

    public ByteBufferChannel(ByteBuffer buffer) {
        myBuffer = buffer;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        byte[] bytes = new byte[byteBuffer.remaining()];
        myBuffer.get(bytes);
        byteBuffer.put(bytes);
        return bytes.length;
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        byte[] bytes = new byte[myBuffer.remaining()];
        byteBuffer.get(bytes);
        myBuffer.put(bytes);
        return bytes.length;
    }

    @Override
    public long position() throws IOException {
        return myBuffer.position();
    }

    @Override
    public SeekableByteChannel position(long l) throws IOException {
        myBuffer.position((int) l);
        return this;
    }

    @Override
    public long size() throws IOException {
        return myBuffer.capacity();
    }

    @Override
    public SeekableByteChannel truncate(long l) throws IOException {
        return this;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {

    }
}
