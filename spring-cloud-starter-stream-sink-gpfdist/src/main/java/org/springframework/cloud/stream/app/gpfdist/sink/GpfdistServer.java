/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.app.gpfdist.sink;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Processor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;
/**
 * Server implementation around reactor and netty providing endpoint
 * where data can be sent using a gpfdist protocol.
 *
 * @author Janne Valkealahti
 */
public class GpfdistServer {

	private final static Log log = LogFactory.getLog(GpfdistServer.class);

	private final Processor<ByteBuf, ByteBuf> processor;
	private final int port;
	private final int flushCount;
	private final int flushTime;
	private final int batchTimeout;
	private final int batchCount;
	private NettyContext server;
	private int localPort = -1;
	private WorkQueueProcessor workProcessor;

	/**
	 * Instantiates a new gpfdist server.
	 *
	 * @param processor the processor
	 * @param port the port
	 * @param flushCount the flush count
	 * @param flushTime the flush time
	 * @param batchTimeout the batch timeout
	 * @param batchCount the batch count
	 */
	public GpfdistServer(Processor<ByteBuf, ByteBuf> processor, int port, int flushCount, int flushTime,
			int batchTimeout, int batchCount) {
		this.processor = processor;
		this.port = port;
		this.flushCount = flushCount;
		this.flushTime = flushTime;
		this.batchTimeout = batchTimeout;
		this.batchCount = batchCount;
	}

	/**
	 * Start a server.
	 *
	 * @return the http server
	 * @throws Exception the exception
	 */
	public synchronized NettyContext start() throws Exception {
		workProcessor = WorkQueueProcessor.create("gpfdist-sink-worker", 8192);
		if (server == null) {
			server = createProtocolListener();
		}
		return server;
	}

	/**
	 * Stop a server.
	 *
	 * @throws Exception the exception
	 */
	public synchronized void stop() throws Exception {
		if (workProcessor != null) {
			workProcessor.onComplete();
		}
		if (server != null) {
			server.dispose();
		}
		workProcessor = null;
		server = null;
	}

	/**
	 * Gets the local port.
	 *
	 * @return the local port
	 */
	public int getLocalPort() {
		return localPort;
	}

	private NettyContext createProtocolListener()
			throws Exception {

		// Create a Flux from a processor which contains incoming data.
		// Microbatch data as windows and flush it into downstream with timeout
		// or when window gets full.
		// Combine windowed data into one ByteBuf which will eventually end
		// up into netty pipeline.
		// workProcessor allows any number of netty channels to come and go
		// to spread a load.
		Flux<ByteBuf> stream = Flux.from(processor)
				.windowTimeout(flushCount, Duration.ofSeconds(flushTime))
				.flatMap(s -> s.reduceWith(Unpooled::buffer, ByteBuf::writeBytes))
				.subscribeWith(workProcessor);

		// Every new gpfdist client connection will have its own channel
		// and subscription to upstream.
		// Data is streamed over this channel until we have taken enough
		// batches or we have a timeout for not enough data from upstream.
		// We process raw data into a format needed for gpfdist and send
		// end of data message.


		NettyContext httpServer = HttpServer
				.create(c -> c.host("0.0.0.0").port(port)
						.eventLoopGroup(new NioEventLoopGroup(10)))
				.newRouter(r -> r.get("/data", (request, response) -> {
					response.chunkedTransfer(false);
					return response
							.addHeader("Content-type", "text/plain")
							.addHeader("Expires", "0")
							.addHeader("X-GPFDIST-VERSION", "Spring Dataflow")
							.addHeader("X-GP-PROTO", "1")
							.addHeader("Cache-Control", "no-cache")
							.addHeader("Connection", "close")
							.send(stream
									.take(batchCount)
									.timeout(Duration.ofSeconds(batchTimeout), Flux.<ByteBuf> empty())
									.concatWith(Flux.just(Unpooled.copiedBuffer(new byte[0])))
									.map(new GpfdistCodecFunction(response.alloc())));
				})).block();

		log.info("Server running using address=[" + httpServer.address() + "]");
		localPort = httpServer.address().getPort();
		return httpServer;
	}

	/**
	 * Function which takes a given ByteBuf having a raw data to be sent
	 * into a pipeline consumed by gpfdist nodes.
	 *
	 * Gpfdist protocol has a special format it understand and we're
	 * only using part of it to send a data. In a nutshell first 5 bytes
	 * are reserved where first is 'D' indicating we're sending data and
	 * next 4 bytes tells how many bytes are expected.
	 *
	 * For example here we have data "1\n".
	 *
	 *          +-------------------------------------------------+
	 *         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
	 *+--------+-------------------------------------------------+----------------+
	 *|00000000| 44 00 00 00 02 31 0a                            |D....1.         |
	 *+--------+-------------------------------------------------+----------------+
	 *
	 * When connection is open from a gpfdist node, there has to be special
	 * end of transmission message which is same as above but we just tell
	 * that we're sending zero bytes.
	 *
	 *         +-------------------------------------------------+
	 *         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
	 *+--------+-------------------------------------------------+----------------+
	 *|00000000| 44 00 00 00 00                                  |D....           |
	 *+--------+-------------------------------------------------+----------------+
	 *
	 */
	private static class GpfdistCodecFunction implements Function<ByteBuf, ByteBuf> {

		final byte[] h1 = Character.toString('D').getBytes(Charset.forName("UTF-8"));
		final ByteBufAllocator alloc;

		public GpfdistCodecFunction(ByteBufAllocator alloc) {
			this.alloc = alloc;
		}

		@Override
		public ByteBuf apply(ByteBuf t) {
			return alloc
					.buffer()
					.writeBytes(h1)
					.writeBytes(ByteBuffer.allocate(4).putInt(t.readableBytes()).array())
					.writeBytes(t);
		}
	}
}
