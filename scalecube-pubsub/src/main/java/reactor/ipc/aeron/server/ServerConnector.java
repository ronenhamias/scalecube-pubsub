/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.ipc.aeron.server;

import io.aeron.Publication;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.AeronWrapper;
import reactor.ipc.aeron.DefaultMessagePublication;
import reactor.ipc.aeron.HeartbeatSender;
import reactor.ipc.aeron.MessagePublication;
import reactor.ipc.aeron.MessageType;
import reactor.ipc.aeron.Protocol;
import reactor.ipc.aeron.RetryTask;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * @author Anatoly Kadyshev
 */
public class ServerConnector implements Disposable {

    private static final Logger logger = Loggers.getLogger(ServerConnector.class);

    private final String category;

    private final Publication clientControlPublication;

    private final int serverSessionStreamId;

    private final UUID connectRequestId;

    private final AeronOptions options;

    private final long sessionId;

    private final HeartbeatSender heartbeatSender;

    private volatile Disposable heartbeatSenderDisposable = () -> {};

    ServerConnector(String category,
                    AeronWrapper wrapper,
                    String clientChannel,
                    int clientControlStreamId,
                    long sessionId,
                    int serverSessionStreamId,
                    UUID connectRequestId,
                    AeronOptions options,
                    HeartbeatSender heartbeatSender) {
        this.category = category;
        this.serverSessionStreamId = serverSessionStreamId;
        this.connectRequestId = connectRequestId;
        this.options = options;
        this.sessionId = sessionId;
        this.heartbeatSender = heartbeatSender;
        this.clientControlPublication = wrapper.addPublication(clientChannel, clientControlStreamId,
                "to send control requests to client", sessionId);
    }

    Mono<Void> connect() {
        return Mono.create(sink -> new RetryTask(
                Schedulers.single(),100,
                options.connectTimeoutMillis() + options.controlBackpressureTimeoutMillis(),
                new SendConnectAckTask(sink), th -> sink.error(
                        new RuntimeException(String.format("Failed to send %s into %s",
                                MessageType.CONNECT_ACK, AeronUtils.format(clientControlPublication)), th))).schedule())

                .then(Mono.fromRunnable(() -> {
                    this.heartbeatSenderDisposable = heartbeatSender.scheduleHeartbeats(clientControlPublication, sessionId)
                            .subscribe(ignore -> { }, th -> { });
                }));
    }

    @Override
    public void dispose() {
        heartbeatSenderDisposable.dispose();

        clientControlPublication.close();
    }

    class SendConnectAckTask implements Callable<Boolean> {

        private final MessagePublication publication;

        private final MonoSink<?> sink;

        SendConnectAckTask(MonoSink<?> sink) {
            this.sink = sink;
            this.publication = new DefaultMessagePublication(clientControlPublication, category, 0, 0);
        }

        @Override
        public Boolean call() throws Exception{
            long result = publication.publish(MessageType.CONNECT_ACK,
                    Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId), sessionId);
            if (result > 0) {
                logger.debug("[{}] Sent {} to {}", category, MessageType.CONNECT_ACK, category, publication.asString());
                sink.success();
                return true;
            } else if (result == Publication.CLOSED) {
                throw new RuntimeException(String.format("Publication %s has been closed", publication.asString()));
            }

            return false;
        }
    }

}
