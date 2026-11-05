/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8;

import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;

// Remoting 客户端的自动插桩入口：生产、批量编码、PushConsumer 消费三个切入点协作。
// 目录名中的 4.8 是编译基线；实际兼容范围由构建中的 Muzzle 配置与目标方法共同约束。
@AutoService(InstrumentationModule.class)
public class RocketMqInstrumentationModule extends InstrumentationModule {
  public RocketMqInstrumentationModule() {
    // 两个名称用于 javaagent 的插桩启停配置，和导出遥测时的 instrumentation scope 名称不同。
    super("rocketmq-client-4.8", "rocketmq-client");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return asList(
        new RocketMqProducerInstrumentation(),
        new RocketMqMessageBatchInstrumentation(),
        new RocketMqConsumerInstrumentation());
  }
}
