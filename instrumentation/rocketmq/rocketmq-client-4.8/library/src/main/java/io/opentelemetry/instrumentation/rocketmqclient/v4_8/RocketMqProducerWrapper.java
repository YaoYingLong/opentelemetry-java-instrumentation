/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;

// library 手动接入时返回 MQProducer 动态代理，在 SDK 编码批次之前注入传播上下文。
// 当前路径使用一个 PRODUCER Send Span 的上下文覆盖批内消息，不创建 javaagent 路径的逐条 Create Span。
final class RocketMqProducerWrapper implements InvocationHandler {

  private final DefaultMQProducer producer;
  private final Instrumenter<SendMessageContext, Void> instrumenter;
  private final TextMapPropagator propagator;

  private RocketMqProducerWrapper(
      DefaultMQProducer producer,
      Instrumenter<SendMessageContext, Void> instrumenter,
      TextMapPropagator propagator) {
    this.producer = producer;
    this.instrumenter = instrumenter;
    this.propagator = propagator;
  }

  @SuppressWarnings(
      "deprecation") // Hook registration requires the deprecated implementation accessor.
  static MQProducer wrap(
      DefaultMQProducer producer,
      Instrumenter<SendMessageContext, Void> instrumenter,
      TextMapPropagator propagator) {
    // 普通发送由此 Hook 负责；代理处理的批次则利用 Hook 回填地址、发送结果等 SDK 信息。
    producer
        .getDefaultMQProducerImpl()
        .registerSendMessageHook(createSendMessageHook(instrumenter));
    return (MQProducer)
        Proxy.newProxyInstance(
            MQProducer.class.getClassLoader(),
            new Class<?>[] {MQProducer.class},
            new RocketMqProducerWrapper(producer, instrumenter, propagator));
  }

  static SendMessageHook createSendMessageHook(
      Instrumenter<SendMessageContext, Void> instrumenter) {
    return new TracingSendMessageHookImpl(instrumenter);
  }

  @Override
  @Nullable
  public Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
    // 只接管首参数声明为 Collection 的 send 重载；生命周期、单条发送及其他方法直接调用原对象。
    if (!method.getName().equals("send")
        || method.getParameterCount() == 0
        || method.getParameterTypes()[0] != Collection.class
        || args == null
        || args[0] == null) {
      return invokeDelegate(method, args);
    }
    List<Message> originals = new ArrayList<>();
    // null 元素和空集合交回 SDK 校验，保留其异常行为，避免在这里凭空创建批量发送操作。
    for (Object item : (Collection<?>) args[0]) {
      if (item == null) {
        return invokeDelegate(method, args);
      }
      originals.add((Message) item);
    }
    if (originals.isEmpty()) {
      return invokeDelegate(method, args);
    }
    // Each encoded batch needs its own carriers when messages are reused before async completion.
    // 同一批 Message 被再次用于发送时，使用不同包装对象保存状态，防止后一次发送覆盖前一次状态。
    List<SendMessage> messages = new ArrayList<>();
    for (Message original : originals) {
      messages.add(new SendMessage(original));
    }

    // envelope 只给遥测提取器提供批次 Topic 等信息；真正发送的是下面替换参数后的 messages 集合。
    // 消息计数单独保存在 BatchSendContext，避免把 envelope 当成普通单条消息。
    Message envelope = new Message();
    envelope.setTopic(messages.get(0).getTopic());
    envelope.setWaitStoreMsgOK(messages.get(0).isWaitStoreMsgOK());
    BatchSendContext request =
        new BatchSendContext(
            envelope, RocketMqNamespaceUtil.getNamespace(producer), messages.size());
    // 从签名判断重载模式，即使调用方传入 null callback，也仍属于异步重载。
    int callbackIndex = -1;
    for (int i = 1; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i] == SendCallback.class) {
        callbackIndex = i;
        break;
      }
    }
    request.setCommunicationMode(
        callbackIndex == -1 ? CommunicationMode.SYNC : CommunicationMode.ASYNC);
    // 捕获调用方 Context，供异步业务 callback 恢复；context 则代表发送操作本身。
    Context parentContext = Context.current();
    Context context =
        instrumenter.shouldStart(parentContext, request)
            ? instrumenter.start(parentContext, request)
            : null;
    SendState state = new SendState(instrumenter, request, context, messages);
    try {
      for (SendMessage message : messages) {
        message.state = state;
        // 必须先逐条写入 properties，再调用原 producer；届时 SDK 才会生成并编码 MessageBatch。
        if (context != null) {
          propagator.inject(context, message, RocketMqProducerWrapper::setProperty);
        }
      }
      Object[] delegateArgs = args.clone();
      delegateArgs[0] = messages;
      if (callbackIndex != -1) {
        // 即使业务 callback 为 null，也返回内部包装 callback，以便异步结束 Span 并清理状态。
        delegateArgs[callbackIndex] = state.wrap((SendCallback) args[callbackIndex], parentContext);
      }
      // 底层发送期间使用操作 Context；作用域随方法返回关闭，异步 Span 可继续活到 callback 完成。
      try (Scope ignored = context == null ? null : context.makeCurrent()) {
        Object result = invokeDelegate(method, delegateArgs);
        if (callbackIndex == -1) {
          request.setSendResult((SendResult) result);
          state.end(null);
        }
        return result;
      }
    } catch (Throwable t) {
      // 包括编码失败、同步发送失败和异步提交失败；结束遥测后继续抛出原异常。
      state.end(t);
      throw t;
    }
  }

  @Nullable
  private Object invokeDelegate(Method method, @Nullable Object[] args) throws Throwable {
    try {
      return method.invoke(producer, args);
    } catch (InvocationTargetException e) {
      // 反射会包装原异常，这里解包以保持调用 MQProducer 时原有的异常类型和语义。
      throw e.getCause();
    }
  }

  private static void setProperty(@Nullable Message message, String key, String value) {
    if (message != null) {
      message.getProperties().put(key, value);
    }
  }

  @Nullable
  static SendState getState(SendMessageContext context) {
    // SDK Hook 拿到的是外层批次，遍历其中的 SendMessage 找到代理提前登记的状态。
    // 未发现包装消息时返回 null，让普通 Hook 正常创建自己的发送操作。
    Message message = context.getMessage();
    if (message instanceof Iterable<?>) {
      for (Object item : (Iterable<?>) message) {
        if (item instanceof SendMessage) {
          SendState state = ((SendMessage) item).state;
          if (state != null) {
            return state;
          }
        }
      }
    }
    return null;
  }

  private static final class SendMessage extends Message {
    private final Message original;
    @Nullable private volatile SendState state;

    private SendMessage(Message original) {
      this.original = original;
      super.setTopic(original.getTopic());
      setBody(original.getBody());
      setFlag(original.getFlag());
      Map<String, String> properties = original.getProperties();
      if (properties == null) {
        properties = new HashMap<>();
        MessageAccessor.setProperties(original, properties);
      }
      // properties 和 body 与原消息共享，保留 SDK 赋予唯一 ID 等可见修改；并非深拷贝消息内容。
      MessageAccessor.setProperties(this, properties);
    }

    @Override
    public void setTopic(String topic) {
      // SDK 在发送过程中可能增删 namespace 前缀，让原消息也观察到相同的 Topic 修改。
      super.setTopic(topic);
      original.setTopic(topic);
    }
  }

  static final class BatchSendContext extends SendMessageContext {
    @Nullable private final String namespace;
    final long messageCount;

    private BatchSendContext(Message message, @Nullable String namespace, long messageCount) {
      setMessage(message);
      this.namespace = namespace;
      this.messageCount = messageCount;
    }

    @Nullable
    @Override
    public String getNamespace() {
      return namespace;
    }
  }

  static final class SendState {
    private final Instrumenter<SendMessageContext, Void> instrumenter;
    private final SendMessageContext request;
    @Nullable private final Context context;
    private final List<SendMessage> messages;
    private final AtomicBoolean ended = new AtomicBoolean();

    private SendState(
        Instrumenter<SendMessageContext, Void> instrumenter,
        SendMessageContext request,
        @Nullable Context context,
        List<SendMessage> messages) {
      this.instrumenter = instrumenter;
      this.request = request;
      this.context = context;
      this.messages = new ArrayList<>(messages);
    }

    void copyFrom(SendMessageContext context) {
      // Span 早于 SDK Hook 请求创建，因此要把底层实际选择的 Broker 和结果补到遥测请求中。
      request.setBrokerAddr(context.getBrokerAddr());
      request.setCommunicationMode(context.getCommunicationMode());
      request.setSendResult(context.getSendResult());
      request.setException(context.getException());
    }

    private SendCallback wrap(@Nullable SendCallback delegate, Context parentContext) {
      // 内部回调总会执行完成处理；只有业务 delegate 非空时才在调用方 Context 下继续调用它。
      return new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          // callback 可以提供最终结果，不要求此前一定进入过发送 Hook。
          request.setSendResult(sendResult);
          end(null);
          if (delegate != null) {
            try (Scope ignored = parentContext.makeCurrent()) {
              delegate.onSuccess(sendResult);
            }
          }
        }

        @Override
        public void onException(Throwable e) {
          end(e);
          if (delegate != null) {
            try (Scope ignored = parentContext.makeCurrent()) {
              delegate.onException(e);
            }
          }
        }
      };
    }

    private void end(@Nullable Throwable error) {
      // 多个完成入口共用此状态，CAS 确保只执行一次 Span 结束和关联清理。
      if (!ended.compareAndSet(false, true)) {
        return;
      }
      for (SendMessage message : messages) {
        message.state = null;
      }
      messages.clear();
      // SDK 字段只接受 Exception，但 Instrumenter 的 error 参数仍传原始 Throwable，包括 Error。
      request.setException(error instanceof Exception ? (Exception) error : null);
      if (context != null) {
        instrumenter.end(context, request, null, error);
      }
    }
  }
}
