package com.redhat.refarch.vertx.lambdaair.edge.mapping;

import io.vertx.core.http.HttpServerRequest;

public interface Mapper {
    String getHostAddress(HttpServerRequest request, String hostAddress);

    boolean initialize();
}
