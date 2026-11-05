/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.FutureCallback;

/**
 * 持有某条消息的 Send Span 上下文，在对应回执成功或失败时结束遥测。
 *
 * <p>context 来自发送入口的 instrumenter.start，而不是完成线程的 Context.current()； 因此异步切换线程后仍能结束原来的
 * Span。这里不持有或跨线程关闭 Scope。
 */
public class SendSpanFinishingCallback implements FutureCallback<SendReceiptImpl> {
  private final Instrumenter<PublishingMessageImpl, SendReceiptImpl> instrumenter;
  private final Context context;
  private final PublishingMessageImpl message;

  public SendSpanFinishingCallback(
      Instrumenter<PublishingMessageImpl, SendReceiptImpl> instrumenter,
      Context context,
      PublishingMessageImpl message) {
    this.instrumenter = instrumenter;
    this.context = context;
    this.message = message;
  }

  @Override
  public void onSuccess(SendReceiptImpl sendReceipt) {
    instrumenter.end(context, message, sendReceipt, null);
  }

  @Override
  public void onFailure(Throwable t) {
    instrumenter.end(context, message, null, t);
  }
}
