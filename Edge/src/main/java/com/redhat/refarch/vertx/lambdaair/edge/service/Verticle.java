package com.redhat.refarch.vertx.lambdaair.edge.service;

import com.redhat.refarch.vertx.lambdaair.edge.mapping.PropertyMapper;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.client.HttpRequest;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import rx.Observable;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Verticle extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Verticle.class.getName());
    private static final String JS_FILE_NAME = "/edge/routing.js";
    private static final String JS_BUS_ADDRESS = "routing.js";

    private WebClient client;
    private Map<String, String> customMappers = getCustomMappers();

    @Override
    public void start(io.vertx.core.Future<Void> startFuture) {
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

        DeploymentOptions options = new DeploymentOptions().setWorker(true);
        List<Observable<String>> deploymentObservables = new ArrayList<>();
        deploymentObservables.add( deployVerticle(startFuture, PropertyMapper.class.getName(), options) );
        customMappers.values().forEach(name -> deploymentObservables.add( deployVerticle(startFuture, name, options) ));
        Observable<HttpServer> httpServerObservable = Observable.zip(deploymentObservables, objects -> startFuture).flatMap((this::deployHttpServer));
        httpServerObservable.subscribe(httpServer -> logger.info("Running http server on port " + httpServer.actualPort()));
    }

    private Observable<String> deployVerticle(Future<Void> future, String name, DeploymentOptions options) {
        return vertx.rxDeployVerticle(name, options).doOnError(future::fail).toObservable();
    }

    private Observable<HttpServer> deployHttpServer(Future future) {
        Router router = Router.router(vertx);
        router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
        router.route().handler(BodyHandler.create());
        router.route("/*").handler(this::proxy);
        router.route().failureHandler(rc -> {
            logger.log(Level.SEVERE, "Error proxying request", rc.failure());
            rc.response().setStatusCode(500).setStatusMessage(rc.failure().getMessage()).end();
        });
        HttpServer httpServer = vertx.createHttpServer(new HttpServerOptions());
        httpServer.requestHandler(router::accept);
        int port = config().getInteger("http.port", 8080);
        return httpServer.rxListen(port).doOnError(future::fail).toObservable();
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
        getHostAddress(request).subscribe(proxiedURL ->
        {
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
        });
    }

    private Observable<String> getHostAddress(HttpServerRequest request) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("scheme", request.scheme());
        jsonObject.put("host", request.host());
        jsonObject.put("path", request.path());
        jsonObject.put("query", request.query());
        jsonObject.put("forwarded-for", request.getHeader("uberctx-forwarded-for"));
        Observable<Message<JsonObject>> hostAddressObservable = getHostAddressObservable(PropertyMapper.class.getName(), jsonObject);
        for (String busAddress : customMappers.keySet()) {
            hostAddressObservable = hostAddressObservable.flatMap(message -> getHostAddressObservable(busAddress, message.body()));
        }
        return hostAddressObservable.map(mapMessage -> constructURL(mapMessage.body()));
    }

    private String constructURL(JsonObject jsonObject) {
        String scheme = jsonObject.getString("scheme");
        String host = jsonObject.getString("host");
        String path = jsonObject.getString("path");
        String query = jsonObject.getString("query");
        String url = scheme + "://" + host + path;
        if (query != null) {
            url += '?' + query;
        }
        return url;
    }

    private Map<String, String> getCustomMappers() {
        Map<String, String> mappers = new HashMap<>();
        if (new File(JS_FILE_NAME).exists())
        {
            mappers.put(JS_BUS_ADDRESS, JS_FILE_NAME);
        }
        return mappers;
    }

    private Observable<Message<JsonObject>> getHostAddressObservable(String busAddress, JsonObject jsonObject) {
        return vertx.eventBus().<JsonObject>rxSend(busAddress, jsonObject).toObservable();
    }
}
