/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;

/**
 * RocketMQ gRPC 客户端插桩的注册入口，由 Agent 通过 SPI 发现。
 *
 * <p>发送链路依次经过消息构造、发送和异步回执；PushConsumer 分别记录接收与监听器处理， SimpleConsumer
 * 则在主动拉取的入口记录接收。下面的类型插桩共同完成这些生命周期的衔接。
 */
@AutoService(InstrumentationModule.class)
public class RocketMqInstrumentationModule extends InstrumentationModule {
  public RocketMqInstrumentationModule() {
    // 同时提供带版本和不带版本的启用名称；实际匹配的 SDK 类型由各 TypeInstrumentation 决定。
    super("rocketmq-client-5.0", "rocketmq-client");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    // 这里是注册清单，列表顺序不代表运行时调用顺序；具体时机由匹配的方法决定。
    return asList(
        new PublishingMessageImplInstrumentation(),
        // 拦截ProducerImpl的send0和sendAsync方法
        new ProducerImplInstrumentation(),
        // 拦截面向单个队列的异步接收ConsumerImpl的receiveMessage方法
        new ConsumerImplInstrumentation(),
        // 拦截ConsumeService的构造方法，给构造函数中的参数MessageListener添加warp
        new ConsumeServiceInstrumentation(),
        // 拦截SimpleConsumerImpl的receive0方法
        new SimpleConsumerImplInstrumentation());
  }
}
