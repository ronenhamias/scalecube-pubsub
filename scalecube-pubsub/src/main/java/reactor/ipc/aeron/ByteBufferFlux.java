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
package reactor.ipc.aeron;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;

/**
 * @author Anatoly Kadyshev
 */
public final class ByteBufferFlux extends Flux<ByteBuffer> {

    private final Publisher<ByteBuffer> source;

    public static ByteBufferFlux from(String... data) {
        return new ByteBufferFlux(Flux.fromArray(data).map(AeronUtils::stringToByteBuffer));
    }

    public ByteBufferFlux(Publisher<ByteBuffer> source) {
        this.source = source;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuffer> s) {
        source.subscribe(s);
    }

    public Flux<String> asString() {
        return map(AeronUtils::byteBufferToString);
    }
}
