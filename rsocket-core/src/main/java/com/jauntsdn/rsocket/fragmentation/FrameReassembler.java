/*
 * Copyright 2015-2018 the original author or authors.
 *
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

package com.jauntsdn.rsocket.fragmentation;

import com.jauntsdn.rsocket.frame.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.SynchronousSink;

/**
 * The implementation of the RSocket reassembly behavior.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
final class FrameReassembler implements Disposable {
  private static final Logger logger = LoggerFactory.getLogger(FrameReassembler.class);

  final IntObjectMap<ByteBuf> headers;
  final IntObjectMap<CompositeByteBuf> metadata;
  final IntObjectMap<CompositeByteBuf> data;

  private final ByteBufAllocator allocator;
  private final int frameSizeLimit;
  /*written on eventloop thread, read on client threads*/
  private volatile boolean isDisposed;

  public FrameReassembler(ByteBufAllocator allocator, int frameSizeLimit) {
    this.allocator = allocator;
    this.frameSizeLimit = frameSizeLimit;
    this.headers = new IntObjectHashMap<>();
    this.metadata = new IntObjectHashMap<>();
    this.data = new IntObjectHashMap<>();
  }

  @Override
  public void dispose() {
    if (!isDisposed) {
      isDisposed = true;
      for (ByteBuf byteBuf : headers.values()) {
        ReferenceCountUtil.safeRelease(byteBuf);
      }
      headers.clear();

      for (ByteBuf byteBuf : metadata.values()) {
        ReferenceCountUtil.safeRelease(byteBuf);
      }
      metadata.clear();

      for (ByteBuf byteBuf : data.values()) {
        ReferenceCountUtil.safeRelease(byteBuf);
      }
      data.clear();
    }
  }

  @Override
  public boolean isDisposed() {
    return isDisposed;
  }

  @Nullable
  private ByteBuf getHeader(int streamId) {
    return headers.get(streamId);
  }

  private CompositeByteBuf getMetadata(int streamId) {
    CompositeByteBuf byteBuf = metadata.get(streamId);

    if (byteBuf == null) {
      byteBuf = allocator.compositeBuffer();
      metadata.put(streamId, byteBuf);
    }

    return byteBuf;
  }

  private CompositeByteBuf getData(int streamId) {
    CompositeByteBuf byteBuf = data.get(streamId);

    if (byteBuf == null) {
      byteBuf = allocator.compositeBuffer();
      data.put(streamId, byteBuf);
    }

    return byteBuf;
  }

  @Nullable
  private ByteBuf removeHeader(int streamId) {
    return headers.remove(streamId);
  }

  @Nullable
  private CompositeByteBuf removeMetadata(int streamId) {
    return metadata.remove(streamId);
  }

  @Nullable
  private CompositeByteBuf removeData(int streamId) {
    return data.remove(streamId);
  }

  private void putHeader(int streamId, ByteBuf header) {
    headers.put(streamId, header);
  }

  private void cancelAssemble(int streamId) {
    ByteBuf header = removeHeader(streamId);
    CompositeByteBuf metadata = removeMetadata(streamId);
    CompositeByteBuf data = removeData(streamId);

    if (header != null) {
      ReferenceCountUtil.safeRelease(header);
    }

    if (metadata != null) {
      ReferenceCountUtil.safeRelease(metadata);
    }

    if (data != null) {
      ReferenceCountUtil.safeRelease(data);
    }
  }

  private boolean handleNoFollowsFlag(ByteBuf frame, SynchronousSink<ByteBuf> sink, int streamId) {
    ByteBuf header = removeHeader(streamId);
    if (header == null) {
      sink.next(frame);
      return true;
    }
    ByteBuf assembledFrame =
        FrameHeaderFlyweight.hasMetadata(header)
            ? assembleFrameWithMetadata(frame, streamId, header)
            : FragmentationFlyweight.encode(allocator, header, assembleData(frame, streamId));
    frame.release();
    if (exceedsSizeLimit(assembledFrame)) {
      assembledFrame.release();
      return false;
    }
    sink.next(assembledFrame);
    return true;
  }

  boolean handleFollowsFlag(ByteBuf frame, int streamId, FrameType frameType) {
    int assembledSize = frame.readableBytes();
    ByteBuf header = getHeader(streamId);
    if (header == null) {
      header = frame.copy(frame.readerIndex(), FrameHeaderFlyweight.size());

      if (frameType == FrameType.REQUEST_CHANNEL || frameType == FrameType.REQUEST_STREAM) {
        int i = RequestChannelFrameFlyweight.initialRequestN(frame);
        header.writeInt(i);
      }
      putHeader(streamId, header);
    }
    int payloadSize = 0;
    if (FrameHeaderFlyweight.hasMetadata(frame)) {
      CompositeByteBuf compositeMetadata = getMetadata(streamId);
      assembledSize = addFragmentSize(assembledSize, compositeMetadata);
      if (assembledSize < 0) {
        frame.release();
        return false;
      }
      ByteBuf metadata;
      switch (frameType) {
        case REQUEST_FNF:
          metadata = RequestFireAndForgetFrameFlyweight.metadata(frame);
          break;
        case REQUEST_STREAM:
          metadata = RequestStreamFrameFlyweight.metadata(frame);
          break;
        case REQUEST_RESPONSE:
          metadata = RequestResponseFrameFlyweight.metadata(frame);
          break;
        case REQUEST_CHANNEL:
          metadata = RequestChannelFrameFlyweight.metadata(frame);
          break;
          // Payload and synthetic types
        case PAYLOAD:
        case NEXT:
        case NEXT_COMPLETE:
        case COMPLETE:
          metadata = PayloadFrameFlyweight.metadata(frame);
          break;
        default:
          frame.release();
          throw new IllegalStateException("unsupported fragment type");
      }
      payloadSize += metadata.readableBytes();
      compositeMetadata.addComponents(true, metadata.retain());
    }

    CompositeByteBuf compositeData = getData(streamId);
    assembledSize = addFragmentSize(assembledSize, compositeData);
    if (assembledSize < 0) {
      frame.release();
      return false;
    }

    ByteBuf data;
    switch (frameType) {
      case REQUEST_FNF:
        data = RequestFireAndForgetFrameFlyweight.data(frame);
        break;
      case REQUEST_STREAM:
        data = RequestStreamFrameFlyweight.data(frame);
        break;
      case REQUEST_RESPONSE:
        data = RequestResponseFrameFlyweight.data(frame);
        break;
      case REQUEST_CHANNEL:
        data = RequestChannelFrameFlyweight.data(frame);
        break;
        // Payload and synthetic types
      case PAYLOAD:
      case NEXT:
      case NEXT_COMPLETE:
      case COMPLETE:
        data = PayloadFrameFlyweight.data(frame);
        break;
      default:
        // unreachable - handled by similar code block for metadata
        throw new IllegalStateException("unsupported fragment type");
    }
    if (payloadSize + data.readableBytes() == 0) {
      frame.release();
      throw new IllegalArgumentException(
          "received frame fragment with empty payload, and FOLLOWS flag set");
    }
    compositeData.addComponents(true, data.retain());
    frame.release();
    return true;
  }

  boolean reassembleFrame(ByteBuf frame, SynchronousSink<ByteBuf> sink) {
    try {
      if (isDisposed) {
        ReferenceCountUtil.release(frame);
        return true;
      }
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      int streamId = FrameHeaderFlyweight.streamId(frame);
      switch (frameType) {
        case CANCEL:
        case ERROR:
          cancelAssemble(streamId);
          break;
        default:
      }

      if (!frameType.isFragmentable()) {
        sink.next(frame);
        return true;
      }

      boolean hasFollows = FrameHeaderFlyweight.hasFollows(frame);
      boolean success =
          hasFollows
              ? handleFollowsFlag(frame, streamId, frameType)
              : handleNoFollowsFlag(frame, sink, streamId);
      if (!success) {
        return false;
      }
    } catch (Throwable t) {
      logger.error("error reassemble frame", t);
      sink.error(t);
    }
    return true;
  }

  private boolean exceedsSizeLimit(ByteBuf assembledFrame) {
    return assembledFrame.readableBytes() > frameSizeLimit;
  }

  private int addFragmentSize(int curSize, ByteBuf fragment) {
    int size = curSize + fragment.readableBytes();
    if (size < 0 || size > frameSizeLimit) {
      return -1;
    }
    return size;
  }

  private ByteBuf assembleFrameWithMetadata(ByteBuf frame, int streamId, ByteBuf header) {
    ByteBuf metadata;
    CompositeByteBuf cm = removeMetadata(streamId);
    if (cm != null) {
      metadata = cm.addComponents(true, PayloadFrameFlyweight.metadata(frame).retain());
    } else {
      metadata = PayloadFrameFlyweight.metadata(frame).retain();
    }

    ByteBuf data = assembleData(frame, streamId);

    return FragmentationFlyweight.encode(allocator, header, metadata, data);
  }

  private ByteBuf assembleData(ByteBuf frame, int streamId) {
    ByteBuf data;
    CompositeByteBuf cd = removeData(streamId);
    if (cd != null) {
      cd.addComponents(true, PayloadFrameFlyweight.data(frame).retain());
      data = cd;
    } else {
      data = Unpooled.EMPTY_BUFFER;
    }

    return data;
  }
}
