package com.jauntsdn.rsocket.transport.netty;

import com.jauntsdn.rsocket.ConnectionSetupPayload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.SocketAcceptor;
import com.jauntsdn.rsocket.exceptions.RejectedSetupException;
import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.transport.netty.client.TcpClientTransport;
import com.jauntsdn.rsocket.transport.netty.client.WebsocketClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.CloseableChannel;
import com.jauntsdn.rsocket.transport.netty.server.TcpServerTransport;
import com.jauntsdn.rsocket.transport.netty.server.WebsocketServerTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class SetupRejectionTest {

  /*
  TODO Fix this test
  @DisplayName(
      "Rejecting setup by server causes requester RSocket disposal and RejectedSetupException")
  @ParameterizedTest
  @MethodSource(value = "transports")*/
  void rejectSetupTcp(
      Function<InetSocketAddress, ServerTransport<CloseableChannel>> serverTransport,
      Function<InetSocketAddress, ClientTransport> clientTransport) {

    String errorMessage = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMessage);
    Mono<RSocket> serverRequester = acceptor.requesterRSocket();

    CloseableChannel channel =
        RSocketFactory.receive()
            .acceptor(acceptor)
            .transport(serverTransport.apply(new InetSocketAddress("localhost", 0)))
            .start()
            .block(Duration.ofSeconds(5));

    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket clientRequester =
        RSocketFactory.connect()
            .errorConsumer(errorConsumer)
            .transport(clientTransport.apply(channel.address()))
            .start()
            .block(Duration.ofSeconds(5));

    StepVerifier.create(errorConsumer.errors().next())
        .expectNextMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.onClose()).expectComplete().verify(Duration.ofSeconds(5));

    StepVerifier.create(serverRequester.flatMap(socket -> socket.onClose()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.requestResponse(DefaultPayload.create("test")))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    channel.dispose();
  }

  static Stream<Arguments> transports() {
    Function<InetSocketAddress, ServerTransport<CloseableChannel>> tcpServer =
        TcpServerTransport::create;
    Function<InetSocketAddress, ServerTransport<CloseableChannel>> wsServer =
        WebsocketServerTransport::create;
    Function<InetSocketAddress, ClientTransport> tcpClient = TcpClientTransport::create;
    Function<InetSocketAddress, ClientTransport> wsClient = WebsocketClientTransport::create;

    return Stream.of(Arguments.of(tcpServer, tcpClient), Arguments.of(wsServer, wsClient));
  }

  static class ErrorConsumer implements Consumer<Throwable> {
    private final EmitterProcessor<Throwable> errors = EmitterProcessor.create();

    @Override
    public void accept(Throwable t) {
      errors.onNext(t);
    }

    Flux<Throwable> errors() {
      return errors;
    }
  }

  private static class RejectingAcceptor implements SocketAcceptor {
    private final String msg;
    private final EmitterProcessor<RSocket> requesters = EmitterProcessor.create();

    public RejectingAcceptor(String msg) {
      this.msg = msg;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      requesters.onNext(sendingSocket);
      return Mono.error(new RuntimeException(msg));
    }

    public Mono<RSocket> requesterRSocket() {
      return requesters.next();
    }
  }
}
