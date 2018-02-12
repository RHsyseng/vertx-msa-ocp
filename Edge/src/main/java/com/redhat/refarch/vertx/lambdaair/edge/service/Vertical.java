package com.redhat.refarch.vertx.lambdaair.edge.service;

import com.redhat.refarch.vertx.lambdaair.edge.mapping.MappingConfiguration;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Vertical extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());

    private MappingConfiguration mapping = new MappingConfiguration();

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start(startFuture);

        ConfigStoreOptions store = new ConfigStoreOptions();
        store.setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", "app-config.yml"));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));
        retriever.getConfig(result -> startWithConfig(startFuture, result));
    }

    private void startWithConfig(Future<Void> startFuture, AsyncResult<JsonObject> configResult) {
        if (configResult.failed()) {
            throw new IllegalStateException(configResult.cause());
        }
        mergeIn(null, configResult.result().getMap());
        System.getProperties().putAll(config().getMap());

        Router router = Router.router(vertx);
        router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
        router.route("/*").handler(this::proxy);
        HttpServer httpServer = vertx.createHttpServer(new HttpServerOptions().setAcceptBacklog(10000));
        httpServer.requestHandler(router::accept);
        int port = config().getInteger("http.port", 8080);
        httpServer.listen(port, result -> {
            if (result.succeeded()) {
                startFuture.succeeded();
                logger.info("Running http server on port " + result.result().actualPort());
            } else {
                startFuture.fail(result.cause());
            }
        });
    }

    private void mergeIn(String prefix, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key;
            if (prefix == null) {
                key = entry.getKey();
            } else {
                key = prefix + "." + entry.getKey();
            }
            Object value = entry.getValue();
            if (value instanceof Map) {
                //noinspection unchecked
                mergeIn(key, (Map<String, Object>) value);
            } else {
                config().put(key, value);
            }
        }
    }

    private void proxy(RoutingContext routingContext) {
        HttpServerRequest request = routingContext.request();
        HttpServerResponse response = routingContext.response();
        String proxiedURL = mapping.getHostAddress(request);
        logger.fine("Will forward request to " + proxiedURL);

        request.bodyHandler(buffer -> {
            HttpRequest<Buffer> serviceRequest = WebClient.create(vertx).requestAbs(request.method(), proxiedURL);
            serviceRequest.headers().addAll(request.headers());
            serviceRequest.sendBuffer(buffer, asyncResult -> {
                if (asyncResult.succeeded()) {
                    HttpResponse<Buffer> serviceResponse = asyncResult.result();
                    response.setStatusCode(serviceResponse.statusCode()).setStatusMessage(serviceResponse.statusMessage());
                    if (serviceResponse.statusCode() < 300) {
                        response.headers().addAll(serviceResponse.headers());
                    }
                    response.end(serviceResponse.bodyAsBuffer());
                } else {
                    response.setStatusCode(500).setStatusMessage(asyncResult.cause().getMessage()).end();
                }
            });
        }).exceptionHandler(throwable -> {
            logger.log(Level.SEVERE, "Error proxying request", throwable);
            response.setStatusCode(500).setStatusMessage(throwable.getMessage()).end();
        });
    }
}
