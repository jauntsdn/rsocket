package com.jauntsdn.rsocket.examples.transport.tcp.resume;

import com.jauntsdn.rsocket.AbstractRSocket;
import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.resume.ClientResume;
import com.jauntsdn.rsocket.resume.PeriodicResumeStrategy;
import com.jauntsdn.rsocket.resume.ResumeStrategy;
import com.jauntsdn.rsocket.transport.netty.client.TcpClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.CloseableChannel;
import com.jauntsdn.rsocket.transport.netty.server.TcpServerTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ResumeFileTransfer {
  /*amount of file chunks requested by subscriber: n, refilled on n/2 of received items*/
  private static final int PREFETCH_WINDOW_SIZE = 4;

  public static void main(String[] args) {
    RequestCodec requestCodec = new RequestCodec();

    CloseableChannel server =
        RSocketFactory.receive()
            .resume()
            .resumeSessionDuration(Duration.ofMinutes(5))
            .acceptor((setup, rSocket) -> Mono.just(new FileServer(requestCodec)))
            .transport(TcpServerTransport.create("localhost", 8000))
            .start()
            .block();

    RSocket client =
        RSocketFactory.connect()
            .resume()
            .resumeStrategy(
                () -> new VerboseResumeStrategy(new PeriodicResumeStrategy(Duration.ofSeconds(1))))
            .resumeSessionDuration(Duration.ofMinutes(5))
            .transport(TcpClientTransport.create("localhost", 8001))
            .start()
            .block();

    client
        .requestStream(requestCodec.encode(new Request(16, "lorem.txt")))
        .doFinally(s -> server.dispose())
        .subscribe(Files.fileSink("rsocket-examples/out/lorem_output.txt", PREFETCH_WINDOW_SIZE));

    server.onClose().block();
  }

  private static class FileServer extends AbstractRSocket {
    private final RequestCodec requestCodec;

    public FileServer(RequestCodec requestCodec) {
      this.requestCodec = requestCodec;
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      Request request = requestCodec.decode(payload);
      payload.release();
      String fileName = request.getFileName();
      int chunkSize = request.getChunkSize();

      Flux<Long> ticks = Flux.interval(Duration.ofMillis(500)).onBackpressureDrop();

      return Files.fileSource(fileName, chunkSize)
          .map(DefaultPayload::create)
          .zipWith(ticks, (p, tick) -> p);
    }
  }

  private static class VerboseResumeStrategy implements ResumeStrategy {
    private final ResumeStrategy resumeStrategy;

    public VerboseResumeStrategy(ResumeStrategy resumeStrategy) {
      this.resumeStrategy = resumeStrategy;
    }

    @Override
    public Publisher<?> apply(ClientResume clientResume, Throwable throwable) {
      return Flux.from(resumeStrategy.apply(clientResume, throwable))
          .doOnNext(v -> System.out.println("Disconnected. Trying to resume connection..."));
    }
  }

  private static class RequestCodec {

    public Payload encode(Request request) {
      String encoded = request.getChunkSize() + ":" + request.getFileName();
      return DefaultPayload.create(encoded);
    }

    public Request decode(Payload payload) {
      String encoded = payload.getDataUtf8();
      String[] chunkSizeAndFileName = encoded.split(":");
      int chunkSize = Integer.parseInt(chunkSizeAndFileName[0]);
      String fileName = chunkSizeAndFileName[1];
      return new Request(chunkSize, fileName);
    }
  }

  private static class Request {
    private final int chunkSize;
    private final String fileName;

    public Request(int chunkSize, String fileName) {
      this.chunkSize = chunkSize;
      this.fileName = fileName;
    }

    public int getChunkSize() {
      return chunkSize;
    }

    public String getFileName() {
      return fileName;
    }
  }
}
