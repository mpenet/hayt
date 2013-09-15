/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// code Extracted from datastax/java-driver
// https://github.com/datastax/java-driver/blob/2.0/driver-core/src/main/java/com/datastax/driver/core/utils/Bytes.java

package qbits.hayt;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Simple utility methods to make working with bytes (blob) easier.
 */
public final class Hex {

    private Hex() {}

    private static final byte[] charToByte = new byte[256];
    private static final char[] byteToChar = new char[16];
    static {
        for (char c = 0; c < charToByte.length; ++c) {
            if (c >= '0' && c <= '9')
                charToByte[c] = (byte)(c - '0');
            else if (c >= 'A' && c <= 'F')
                charToByte[c] = (byte)(c - 'A' + 10);
            else if (c >= 'a' && c <= 'f')
                charToByte[c] = (byte)(c - 'a' + 10);
            else
                charToByte[c] = (byte)-1;
        }

        for (int i = 0; i < 16; ++i) {
            byteToChar[i] = Integer.toHexString(i).charAt(0);
        }
    }

    /*
     * We use reflexion to get access to a String protected constructor
     * (if available) so we can build avoid copy when creating hex strings.
     * That's stolen from Cassandra's code.
     */
    private static final Constructor<String> stringConstructor;
    static {
        Constructor<String> c;
        try {
            c = String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            c.setAccessible(true);
        } catch (Exception e) {
            c = null;
        }
        stringConstructor = c;
    }

    private static String wrapCharArray(char[] c) {
        if (c == null)
            return null;

        String s = null;
        if (stringConstructor != null) {
            try {
                s = stringConstructor.newInstance(0, c.length, c);
            } catch (Exception e) {
                // Swallowing as we'll just use a copying constructor
            }
        }
        return s == null ? new String(c) : s;
    }

    /**
     * Converts a blob to its CQL hex string representation.
     * <p>
     * A CQL blob string representation consist of the hexadecimal
     * representation of the blob bytes prefixed by "0x".
     *
     * @param bytes the blob/bytes to convert to a string.
     * @return the CQL string representation of {@code bytes}. If {@code bytes}
     * is {@code null}, this method returns {@code null}.
     */
    public static String toHexString(ByteBuffer bytes) {
        if (bytes == null)
            return null;

        if (bytes.remaining() == 0)
            return "0x";

        char[] array = new char[2 * (bytes.remaining() + 1)];
        array[0] = '0';
        array[1] = 'x';
        return toRawHexString(bytes, array, 2);
    }

    /**
     * Converts a blob to its CQL hex string representation.
     * <p>
     * A CQL blob string representation consist of the hexadecimal
     * representation of the blob bytes prefixed by "0x".
     *
     * @param byteArray the blob/bytes array to convert to a string.
     * @return the CQL string representation of {@code bytes}. If {@code bytes}
     * is {@code null}, this method returns {@code null}.
     */
    public static String toHexString(byte[] byteArray) {
        return toHexString(ByteBuffer.wrap(byteArray));
    }

    private static String toRawHexString(ByteBuffer bytes, char[] array, int offset) {
        int size = bytes.remaining();
        int bytesOffset = bytes.position();
        assert array.length >= offset + 2*size;
        for (int i = 0; i < size; i++) {
            int bint = bytes.get(i+bytesOffset);
            array[offset + i * 2] = byteToChar[(bint & 0xf0) >> 4];
            array[offset + 1 + i * 2] = byteToChar[bint & 0x0f];
        }
        return wrapCharArray(array);
    }
}
