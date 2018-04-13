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

import java.net.InetSocketAddress;

import org.junit.Test;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TestListenAddress {


	@Test
	public void testBindZero() throws Exception {
		NettyContext httpServer = HttpServer
				.create()
				.newRouter(r -> r.get("/data", (request, response) -> {
					return response.send(null);
				})).block();
		InetSocketAddress address = httpServer.address();
		assertThat(address, notNullValue());
		assertThat(address.getPort(), not(0));
		httpServer.dispose();
	}
}
