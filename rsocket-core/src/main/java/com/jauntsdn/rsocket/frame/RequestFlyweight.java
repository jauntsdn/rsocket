package com.jauntsdn.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import javax.annotation.Nullable;

class RequestFlyweight {
  FrameType frameType;

  RequestFlyweight(FrameType frameType) {
    this.frameType = frameType;
  }

  ByteBuf encode(
      final ByteBufAllocator allocator,
      final int streamId,
      boolean fragmentFollows,
      @Nullable ByteBuf metadata,
      ByteBuf data) {
    return encode(allocator, streamId, fragmentFollows, false, false, 0, metadata, data);
  }

  ByteBuf encode(
      final ByteBufAllocator allocator,
      final int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      int requestN,
      @Nullable ByteBuf metadata,
      ByteBuf data) {
    int flags = 0;

    if (metadata != null) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    if (fragmentFollows) {
      flags |= FrameHeaderFlyweight.FLAGS_F;
    }

    if (complete) {
      flags |= FrameHeaderFlyweight.FLAGS_C;
    }

    if (next) {
      flags |= FrameHeaderFlyweight.FLAGS_N;
    }

    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, frameType, flags);

    if (requestN > 0) {
      header.writeInt(requestN);
    }

    if (data == null && metadata == null) {
      return header;
    } else if (metadata != null) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
    } else {
      return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
    }
  }

  ByteBuf data(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    int idx = byteBuf.readerIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.readerIndex(idx);
    return data;
  }

  ByteBuf metadata(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  ByteBuf dataWithRequestN(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }

  ByteBuf metadataWithRequestN(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  int initialRequestN(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.skipBytes(FrameHeaderFlyweight.size()).readInt();
    byteBuf.resetReaderIndex();
    return i;
  }
}
