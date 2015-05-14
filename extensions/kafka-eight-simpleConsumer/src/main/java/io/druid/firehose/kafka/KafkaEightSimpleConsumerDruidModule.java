/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.firehose.kafka;

import java.util.List;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;

import io.druid.initialization.DruidModule;

public class KafkaEightSimpleConsumerDruidModule implements DruidModule{
	@Override
	public List<? extends Module> getJacksonModules()
	{
		return ImmutableList.of(
				new SimpleModule("KafkaEightSimpleConsumerFirehoseModule").registerSubtypes(
						new NamedType(KafkaEightSimpleConsumerFirehoseFactory.class, "kafka-0.8-simpleConsumer")
						)
				);
	}

	@Override
	public void configure(Binder binder)
	{

	}
}
