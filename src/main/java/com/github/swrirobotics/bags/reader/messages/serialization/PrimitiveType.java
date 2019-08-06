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

import com.github.swrirobotics.bags.reader.exceptions.UninitializedFieldException;

import java.lang.reflect.InvocationTargetException;

import java.nio.ByteBuffer;

/**
 * Represents a primitive type within a message definition.
 * Primitive types can be read and deserialized directly from
 * a byte stream without knowledge of any other message formats.
 * @param <T> The Java type that the primitive will be
 *           represented as.
 */
public abstract class PrimitiveType<T> implements Field {
    protected T myValue = null;
    protected T myDefaultValue = null;
    private String myName = null;

    /**
     * After {@link #readMessage(ByteBuffer)} has been called, this will return
     * the value that was read.
     * @return The value read for this field.
     * @throws UninitializedFieldException If the field has not been read.
     */
    public T getValue() throws UninitializedFieldException {
        if (myValue == null) {
            throw new UninitializedFieldException();
        }
        return myValue;
    }

    @Override
    public void reset() {
        myValue = myDefaultValue;
    }

    /**
     * Sets the value of a constant field.
     * Setting it to "null" means it is not a constant.
     * @param value The field's constant value.
     */
    public void setDefaultValue(String value) {
        setValue(value);
        myDefaultValue = myValue;
        reset();
    }

    @Override
    public String getName() {
        return myName;
    }

    @Override
    public void setName(String name) {
        myName = name;
    }

    /**
     * Used for setting the value of this field from a string.
     * @param value A String that can be parsed into a value for this type.
     */
    abstract public void setValue(String value);

    /**
     * Sets the value of this field from an object of the same type.
     * @param value The value to set in this field.
     */
    public void setValue(T value) {
        myValue = value;
    }

    /**
     * Creates a copy of this field.  The new field will have the same type,
     * name, and default value, but if this field had previously been read
     * and its value was set, that will not be copied.
     * @return A copy of this field.
     */
    @SuppressWarnings("unchecked")
    public Field copy() {
        PrimitiveType<T> newType;
        try {
            newType = this.getClass().getDeclaredConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            // This should never happen, so there's no need to explicitly
            // throw an exception, but just in case it does...
            throw new RuntimeException(e);
        }

        newType.myName = myName;
        newType.myDefaultValue = myDefaultValue;
        newType.myValue = myDefaultValue;

        return newType;
    }
}
