/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class TestUtil
{
    public static Frame utf8EncodedRequestFrame(final long streamId, final FrameType type, final String data, final long initialRequestN)
    {
        return Frame.fromRequest(streamId, type, new Payload()
        {
            public ByteBuffer getData()
            {
                return byteBufferFromUtf8String(data);
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        }, initialRequestN);
    }

    public static Frame utf8EncodedFrame(final long streamId, final FrameType type, final String data)
    {
        return Frame.from(streamId, type, byteBufferFromUtf8String(data));
    }

    public static Payload utf8EncodedPayload(final String data, final String metadata)
    {
        return new PayloadImpl(data, metadata);
    }

    public static String byteToString(final ByteBuffer byteBuffer)
    {
        final byte[] bytes = new byte[byteBuffer.capacity()];
        byteBuffer.get(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
        return ByteBuffer.wrap(bytes);
    }

    private static class PayloadImpl implements Payload // some JDK shoutout
    {
        private ByteBuffer data;
        private ByteBuffer metadata;

        public PayloadImpl(final String data, final String metadata)
        {
            if (null == data)
            {
                this.data = ByteBuffer.allocate(0);
            }
            else
            {
                this.data = byteBufferFromUtf8String(data);
            }

            if (null == metadata)
            {
                this.metadata = ByteBuffer.allocate(0);
            }
            else
            {
                this.metadata = byteBufferFromUtf8String(metadata);
            }
        }

        public boolean equals(Object obj)
        {
            final Payload rhs = (Payload)obj;

            return (TestUtil.byteToString(data).equals(TestUtil.byteToString(rhs.getData()))) &&
                (TestUtil.byteToString(metadata).equals(TestUtil.byteToString(rhs.getMetadata())));
        }

        public ByteBuffer getData()
        {
            return data;
        }

        public ByteBuffer getMetadata()
        {
            return metadata;
        }
    }

}