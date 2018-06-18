/*
 * Copyright 2016 YANDEX LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2009 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.yandex.clickhouse.util.guava;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.util.Utils;


public class StreamUtils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private static final int BUF_SIZE = 0x1000; // 4K
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    public static String toString(InputStream in) throws IOException {
        return new String(toByteArray(in), UTF_8);
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        return toByteArray(in, false);
    }

    public static byte[] toByteArray(InputStream in, boolean asciiOnly) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out, asciiOnly);
        return out.toByteArray();
    }

    public static long copy(InputStream from, OutputStream to) throws IOException {
        return copy(from, to, false);
    }

    public static long copy(InputStream from, OutputStream to, boolean asciiOnly) throws IOException {
        InputStream in = asciiOnly ? new AsciiOnlyInputStream(from) : from;
        byte[] buf = new byte[BUF_SIZE];
        long total = 0;
        while (true) {
            int r = in.read(buf);
            if (r == -1) {
                break;
            }
            to.write(buf, 0, r);
            total += r;
        }
        return total;
    }

    public static void close(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            log.error("can not close stream: " + e.getMessage());
        }
    }

    public static void close(ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (SQLException e) {
            log.error("can not close resultset: " + e.getMessage());
        }
    }

    /**
     * Filters only ASCII-printable characters, replaces other by '_'
     */
    private static class AsciiOnlyInputStream extends FilterInputStream {
        public AsciiOnlyInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int nextByte = super.read();
            if (nextByte == -1) {
                return nextByte;
            } else if (nextByte >= 32 && nextByte < 127) {
                return nextByte;
            } else {
                return '_';
            }
        }
    }
}
