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

import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Processor;
import reactor.core.publisher.WorkQueueProcessor;

import org.springframework.cloud.stream.app.gpfdist.sink.support.Format;
import org.springframework.cloud.stream.app.gpfdist.sink.support.LoadConfiguration;
import org.springframework.cloud.stream.app.gpfdist.sink.support.LoadFactoryBean;
import org.springframework.cloud.stream.app.gpfdist.sink.support.NetworkUtils;
import org.springframework.cloud.stream.app.gpfdist.sink.support.ReadableTableFactoryBean;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.util.net.DefaultHostInfoDiscovery;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base integration support for using local protocol listener.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractLoadTests {

	protected AnnotationConfigApplicationContext context;

	private Processor<ByteBuf, ByteBuf> processor;

	private GpfdistServer server;

	static class CommonConfig {

		@Bean
		public LoadFactoryBean greenplumLoad(LoadConfiguration loadConfiguration) {
			LoadFactoryBean factory = new LoadFactoryBean();
			factory.setLoadConfiguration(loadConfiguration);
			factory.setDataSource(dataSource());
			return factory;
		}

		@Bean
		public ReadableTableFactoryBean greenplumReadableTable() {
			ReadableTableFactoryBean factory = new ReadableTableFactoryBean();
			DefaultHostInfoDiscovery discovery = new DefaultHostInfoDiscovery();
			factory.setLocations(Arrays.asList(NetworkUtils.getGPFDistUri(discovery.getHostInfo().getAddress(), 8080)));
			factory.setFormat(Format.TEXT);
			return factory;
		}

		@Bean
		public JdbcTemplate jdbcTemplate() {
			return new JdbcTemplate(dataSource());
		}

		@Bean
		public BasicDataSource dataSource() {
			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("org.postgresql.Driver");
			dataSource.setUrl("jdbc:postgresql://mdw/gpadmin");
			dataSource.setUsername("gpadmin");
			dataSource.setPassword("gpadmin");
			return dataSource;
		}

	}

	protected void broadcastData(List<String> data) {
		for (String d : data) {
			processor.onNext(Unpooled.copiedBuffer(d.getBytes()));
		}
	}

	@Before
	public void setup() throws Exception {
		processor = WorkQueueProcessor.create();
		server = new GpfdistServer(processor, 8080, 1, 1, 1, 10);
		server.start();
		context = new AnnotationConfigApplicationContext();
	}

	@After
	public void clean() throws Exception {
		server.stop();
		context.close();
		context = null;
		server = null;
	}

}
