package reactor.ipc.aeron;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import reactor.test.StepVerifier;
import java.time.Duration;

/**
 * @author Anatoly Kadyshev
 */
public class AeronClientTest extends BaseAeronTest {

    private String serverChannel = "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort(13000);

    private String clientChannel = "aeron:udp?endpoint=localhost:" + SocketUtils.findAvailableUdpPort();


    @Test(expected = RuntimeException.class)
    public void testClientCouldNotConnectToServer() {
        AeronClient client = AeronClient.create();
        try {
            client.newHandler((inbound, outbound) -> Mono.never()).block();
        } finally {
            client.dispose();
        }
    }

    @Test
    public void testClientReceivesDataFromServer() {
        AeronServer server = createAeronServer("server");

        server.newHandler((inbound, outbound) ->
                Mono.from(outbound.send(ByteBufferFlux.from("hello1", "2", "3")
                        .log("server")))).block(TIMEOUT);

        ReplayProcessor<String> processor = ReplayProcessor.create();
        AeronClient client = createAeronClient("ReceivesDataFromServer");
        try {
            client.newHandler((inbound, outbound) -> {
                inbound.receive().asString().log("client").subscribe(processor);
                return Mono.never();
            }).block(TIMEOUT);

            StepVerifier.create(processor)
                    .expectNext("hello1", "2", "3")
                    .expectNoEvent(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();
        } finally {
            processor.dispose();
            client.dispose();
        }
    }

    @Test
    public void testTwoClientsReceiveDataFromServer() throws InterruptedException {
        AeronServer server = createAeronServer("server");
        blockAndAddDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(ByteBufferFlux.from("1", "2", "3").log())).subscribe();
            return Mono.never();
        }));

        ReplayProcessor<String> processor1 = ReplayProcessor.create();
        AeronClient client1 = createAeronClient("client-1");
        blockAndAddDisposable(client1.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("client-1").subscribe(processor1);
            return Mono.never();
        }));
        addDisposable(client1);

        ReplayProcessor<String> processor2 = ReplayProcessor.create();
        AeronClient client2 = createAeronClient("client-2");
        blockAndAddDisposable(client2.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("client-2").subscribe(processor2);
            return Mono.never();
        }));
        addDisposable(client2);

        StepVerifier.create(processor1)
                .expectNext("1", "2", "3")
                .expectNoEvent(Duration.ofMillis(100))
                .thenCancel()
                .verify();

        StepVerifier.create(processor2)
                .expectNext("1", "2", "3")
                .expectNoEvent(Duration.ofMillis(100))
                .thenCancel()
                .verify();
    }

    @Test
    public void testClientWith2HandlersReceiveData() {
        AeronServer server = createAeronServer("server-1");

        blockAndAddDisposable(server.newHandler((inbound, outbound) -> {
            Mono.from(outbound.send(ByteBufferFlux.from("1", "2", "3").log("server"))).subscribe();
            return Mono.never();
        }));

        ReplayProcessor<String> processor1 = ReplayProcessor.create();
        AeronClient client = createAeronClient("TwoHandlersReceiveData");
        blockAndAddDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("client-1").subscribe(processor1);
            return Mono.never();
        }));

        ReplayProcessor<String> processor2 = ReplayProcessor.create();
        blockAndAddDisposable(client.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("client-2").subscribe(processor2);
            return Mono.never();
        }));
        addDisposable(client);

        StepVerifier.create(processor1)
                .expectNext("1", "2", "3")
                .expectNoEvent(Duration.ofMillis(100))
                .thenCancel()
                .verify();

        StepVerifier.create(processor2)
                .expectNext("1", "2", "3")
                .expectNoEvent(Duration.ofMillis(100))
                .thenCancel()
                .verify();
    }

    @Test
    public void testClientClosesSessionUponHeartbeatLoss() throws Exception {
        AeronServer server = AeronServer.create("server", options -> {
                options.serverChannel(serverChannel);
                options.heartbeatTimeoutMillis(500);
        });

        Disposable serverHandlerDisposable = server.newHandler((inbound, outbound) ->
                Mono.from(outbound.send(ByteBufferFlux.from("hello1", "2", "3")
                        .delayElements(Duration.ofSeconds(1))
                        .log("server")))).block(TIMEOUT);

        ReplayProcessor<String> processor = ReplayProcessor.create();
        AeronClient client = AeronClient.create("client", options -> {
            options.clientChannel(clientChannel);
            options.serverChannel(serverChannel);
            options.heartbeatTimeoutMillis(500);
        });
        addDisposable(client);
        client.newHandler((inbound, outbound) -> {
            inbound.receive().asString().log("client").subscribe(processor);
            return Mono.never();
        }).block(TIMEOUT);

        processor.elementAt(2).block(Duration.ofSeconds(4));

        serverHandlerDisposable.dispose();

        Thread.sleep(1500);
    }

    private AeronClient createAeronClient(String name) {
        return AeronClient.create(name, options -> {
                options.clientChannel(clientChannel);
                options.serverChannel(serverChannel);
        });
    }

    private AeronServer createAeronServer(String name) {
        return AeronServer.create(name, options ->
                options.serverChannel(serverChannel));
    }

}
