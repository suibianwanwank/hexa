/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ccsu.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;
import io.protostuff.JsonInput;
import io.protostuff.JsonInputException;
import io.protostuff.JsonOutput;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.InputStream;

public class JsonIOUtils {
    private JsonIOUtils() {
    }

    public static final class Factory
            extends JsonFactory {
        /**
         * Needed by jackson's internal utf8 stream parser.
         */
        public ByteQuadsCanonicalizer getRootByteSymbols() {
            return _byteSymbolCanonicalizer;
        }
    }

    public static final Factory DEFAULT_JSON_FACTORY = new Factory();

    static {
        // disable auto-close to have same behavior as protostuff-core utility io methods
        DEFAULT_JSON_FACTORY.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
        DEFAULT_JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    public static <T> void writeTo(JsonGenerator generator, T message, Schema<T> schema,
                                   boolean numeric) throws IOException {
        generator.writeStartObject();

        final JsonOutput output = new JsonOutput(generator, numeric, schema);
        schema.writeTo(output, message);
        if (output.isLastRepeated()) {
            generator.writeEndArray();
        }

        generator.writeEndObject();
    }

    public static <T> void mergeFrom(JsonParser parser, T message, Schema<T> schema,
                                     boolean numeric) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            throw new JsonInputException("Expected token: { but was "
                    + parser.getCurrentToken() + " on message "
                    + schema.messageFullName());
        }

        schema.mergeFrom(new JsonInput(parser, numeric), message);

        if (parser.getCurrentToken() != JsonToken.END_OBJECT) {
            throw new JsonInputException("Expected token: } but was "
                    + parser.getCurrentToken() + " on message "
                    + schema.messageFullName());
        }
    }

    public static UTF8StreamJsonParser newJsonParser(InputStream in, byte[] buf,
                                                     int offset, int limit) {
        return newJsonParser(in, buf, offset, limit, false,
                new IOContext(DEFAULT_JSON_FACTORY._getBufferRecycler(), in,
                        false));
    }

    static UTF8StreamJsonParser newJsonParser(InputStream in, byte[] buf,
                                              int offset, int limit, boolean bufferRecyclable, IOContext context) {
        return new UTF8StreamJsonParser(context,
                DEFAULT_JSON_FACTORY.getParserFeatures(), in,
                DEFAULT_JSON_FACTORY.getCodec(),
                DEFAULT_JSON_FACTORY.getRootByteSymbols().makeChild(1),
                buf, offset, limit, bufferRecyclable);
    }
}
