package com.jauntsdn.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.ofSeconds;

import com.jauntsdn.rsocket.AbstractRSocket;
import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.lease.Lease;
import com.jauntsdn.rsocket.lease.LeaseStats;
import com.jauntsdn.rsocket.lease.Leases;
import com.jauntsdn.rsocket.transport.netty.client.TcpClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.CloseableChannel;
import com.jauntsdn.rsocket.transport.netty.server.TcpServerTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import java.util.Date;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseExample {
  private static final String SERVER_TAG = "server";
  private static final String CLIENT_TAG = "client";

  public static void main(String[] args) {

    CloseableChannel server =
        RSocketFactory.receive()
            .lease(
                () ->
                    Leases.<NoopStats>create()
                        .sender(new LeaseSender(SERVER_TAG, 7_000, 5))
                        .receiver(new LeaseReceiver(SERVER_TAG))
                        .stats(new NoopStats()))
            .acceptor((setup, sendingRSocket) -> Mono.just(new ServerAcceptor(sendingRSocket)))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    RSocket clientRSocket =
        RSocketFactory.connect()
            .lease(
                () ->
                    Leases.<NoopStats>create()
                        .sender(new LeaseSender(CLIENT_TAG, 3_000, 5))
                        .receiver(new LeaseReceiver(CLIENT_TAG)))
            .acceptor(rSocket -> new ClientAcceptor())
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();

    Flux.interval(ofSeconds(1))
        .flatMap(
            signal -> {
              System.out.println("Client requester availability: " + clientRSocket.availability());
              return clientRSocket
                  .requestResponse(DefaultPayload.create("Client request " + new Date()))
                  .doOnError(err -> System.out.println("Client request error: " + err))
                  .onErrorResume(err -> Mono.empty());
            })
        .subscribe(resp -> System.out.println("Client requester response: " + resp.getDataUtf8()));

    clientRSocket.onClose().block();
    server.dispose();
  }

  private static class LeaseSender implements Function<Optional<NoopStats>, Flux<Lease>> {
    private final String tag;
    private final int ttlMillis;
    private final int allowedRequests;

    public LeaseSender(String tag, int ttlMillis, int allowedRequests) {
      this.tag = tag;
      this.ttlMillis = ttlMillis;
      this.allowedRequests = allowedRequests;
    }

    @Override
    public Flux<Lease> apply(Optional<NoopStats> leaseStats) {
      System.out.println(
          String.format("%s stats are %s", tag, leaseStats.isPresent() ? "present" : "absent"));
      return Flux.interval(ofSeconds(1), ofSeconds(10))
          .onBackpressureLatest()
          .map(
              tick -> {
                System.out.println(
                    String.format(
                        "%s responder sends new leases: ttl: %d, requests: %d",
                        tag, ttlMillis, allowedRequests));
                return Lease.create(ttlMillis, allowedRequests);
              });
    }
  }

  private static class LeaseReceiver implements Consumer<Flux<Lease>> {
    private final String tag;

    public LeaseReceiver(String tag) {
      this.tag = tag;
    }

    @Override
    public void accept(Flux<Lease> receivedLeases) {
      receivedLeases.subscribe(
          l ->
              System.out.println(
                  String.format(
                      "%s received leases - ttl: %d, requests: %d",
                      tag, l.getTimeToLiveMillis(), l.getAllowedRequests())));
    }
  }

  private static class NoopStats implements LeaseStats {

    @Override
    public void onEvent(EventType eventType) {}
  }

  private static class ClientAcceptor extends AbstractRSocket {
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.just(DefaultPayload.create("Client Response " + new Date()));
    }
  }

  private static class ServerAcceptor extends AbstractRSocket {
    private final RSocket senderRSocket;

    public ServerAcceptor(RSocket senderRSocket) {
      this.senderRSocket = senderRSocket;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      System.out.println("Server requester availability: " + senderRSocket.availability());
      senderRSocket
          .requestResponse(DefaultPayload.create("Server request " + new Date()))
          .doOnError(err -> System.out.println("Server request error: " + err))
          .onErrorResume(err -> Mono.empty())
          .subscribe(
              resp -> System.out.println("Server requester response: " + resp.getDataUtf8()));

      return Mono.just(DefaultPayload.create("Server Response " + new Date()));
    }
  }
}
