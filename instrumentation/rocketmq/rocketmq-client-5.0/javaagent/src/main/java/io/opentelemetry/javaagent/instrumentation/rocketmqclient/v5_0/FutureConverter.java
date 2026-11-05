/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.FutureCallback;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.MoreExecutors;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.SettableFuture;

/**
 * 将一个“整批结果的 Future”拆成“每条结果各自的 Future”，供各条消息独立结束 Send Span。
 *
 * <p>这里只分发完成信号，不拆分网络请求。调用方传入消息数量，成功结果按列表下标对应； SDK 的发送完成路径负责检查回执数量，整批失败时所有子 Future 使用同一个异常完成。
 */
public class FutureConverter {
  private FutureConverter() {}

  public static <T> List<SettableFuture<T>> convert(SettableFuture<List<T>> future, int num) {
    List<SettableFuture<T>> futures = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      SettableFuture<T> f = SettableFuture.create();
      futures.add(f);
    }
    ListFutureCallback<T> futureCallback = new ListFutureCallback<>(futures);
    // 不另建线程；若原 Future 已完成，子 Future 可能在 convert 返回前就已完成。
    Futures.addCallback(future, futureCallback, MoreExecutors.directExecutor());
    return futures;
  }

  private static class ListFutureCallback<T> implements FutureCallback<List<T>> {
    private final List<SettableFuture<T>> futures;

    private ListFutureCallback(List<SettableFuture<T>> futures) {
      this.futures = futures;
    }

    @Override
    public void onSuccess(List<T> result) {
      for (int i = 0; i < result.size(); i++) {
        futures.get(i).set(result.get(i));
      }
    }

    @Override
    public void onFailure(Throwable t) {
      for (SettableFuture<T> future : futures) {
        future.setException(t);
      }
    }
  }
}
