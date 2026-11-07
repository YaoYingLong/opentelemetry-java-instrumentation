/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.grpc.override;

import io.grpc.Context;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.ContextStorageBridge;

/**
 * Override class for gRPC to pick up this class to replace the default {@link Context.Storage} with
 * an OpenTelemetry bridge.
 *
 * <p>纯 library 接入通过 gRPC 约定的完整类名发现此入口，无需 Agent 改写 Context.storage()。 这里显式启用 gRPC deadline 传播；Agent
 * 的入口则包裹原 Storage，并采用不同的传播隔离策略。
 */
public final class ContextStorageOverride extends Context.Storage {

  private static final Context.Storage delegate = new ContextStorageBridge(true);

  @Override
  public Context doAttach(Context toAttach) {
    return delegate.doAttach(toAttach);
  }

  @Override
  public void detach(Context toDetach, Context toRestore) {
    delegate.detach(toDetach, toRestore);
  }

  @Override
  public Context current() {
    return delegate.current();
  }
}
