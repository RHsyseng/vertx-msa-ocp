package com.redhat.refarch.vertx.lambdaair.edge.service;

import com.redhat.refarch.vertx.lambdaair.edge.mapping.MappingConfiguration;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
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
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Vertical extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());

    private MappingConfiguration mapping = new MappingConfiguration();
    private WebClient client;

    @Override
    public void start(Future<Void> startFuture) {
        client = WebClient.create(vertx);
        ConfigStoreOptions store = new ConfigStoreOptions();
        store.setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", "app-config.yml"));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));
        retriever.getConfig(result -> startWithConfig(startFuture, result));
    }

    private void startWithConfig(Future<Void> startFuture, AsyncResult<JsonObject> configResult) {
        if (configResult.failed()) {
            startFuture.fail(configResult.cause());
        }
        mergeIn(null, configResult.result().getMap());
        System.getProperties().putAll(config().getMap());

        Router router = Router.router(vertx);
        router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
        router.route().handler(BodyHandler.create());
        router.route("/*").handler(this::proxy);
        router.route().failureHandler(rc -> {
            logger.log(Level.SEVERE, "Error proxying request", rc.failure());
            rc.response().setStatusCode(500).setStatusMessage(rc.failure().getMessage()).end();
        });
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

        Buffer body = routingContext.getBody();

        HttpRequest<Buffer> serviceRequest = client.requestAbs(request.method(), proxiedURL);
        serviceRequest.headers().addAll(request.headers());
        serviceRequest.sendBuffer(body, asyncResult -> {
            if (asyncResult.succeeded()) {
                HttpResponse<Buffer> serviceResponse = asyncResult.result();
                response.setStatusCode(serviceResponse.statusCode())
                    .setStatusMessage(serviceResponse.statusMessage());
                if (serviceResponse.statusCode() < 300) {
                    response.headers().addAll(serviceResponse.headers());
                }
                response.end(serviceResponse.bodyAsBuffer());
            } else {
                response.setStatusCode(500).setStatusMessage(asyncResult.cause().getMessage()).end();
            }
        });
    }
}
