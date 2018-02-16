package com.redhat.refarch.vertx.lambdaair.flights.service;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.uber.jaeger.Configuration;
import com.uber.jaeger.Span;
import io.opentracing.contrib.vertx.ext.web.TracingHandler;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.opentracing.contrib.vertx.ext.web.TracingHandler.CURRENT_SPAN;

public class Vertical extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());
    private WebClient webClient;

    @Override
    public void start() {
        webClient = WebClient.create(vertx);
        Json.mapper.registerModule(new JavaTimeModule());
        ConfigStoreOptions store = new ConfigStoreOptions();
        store.setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", "app-config.yml"));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));
        retriever.getConfig(this::startWithConfig);
    }

    private void startWithConfig(AsyncResult<JsonObject> configResult) {
        if (configResult.failed()) {
            throw new IllegalStateException(configResult.cause());
        }
        mergeIn(null, configResult.result().getMap());

        DeploymentOptions deploymentOptions = new DeploymentOptions().setWorker(true);
        vertx.deployVerticle(new FlightSchedulingService(), deploymentOptions, deployResult -> {
            if (deployResult.succeeded()) {
                Router router = Router.router(vertx);
                router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
                router.get("/query").handler(this::query);
                setupTracing(router);
                HttpServer httpServer = vertx.createHttpServer();
                httpServer.requestHandler(router::accept);
                int port = config().getInteger("http.port", 8080);
                httpServer.listen(port);
            } else {
                logger.log(Level.SEVERE, "Unable to deploy verticle", deployResult.cause());
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

    private void setupTracing(Router router) {
        Configuration.SamplerConfiguration samplerConfiguration = new Configuration.SamplerConfiguration(config().getString("JAEGER_SAMPLER_TYPE"), config().getDouble("JAEGER_SAMPLER_PARAM"), config().getString("JAEGER_SAMPLER_MANAGER_HOST_PORT"));
        Configuration.ReporterConfiguration reporterConfiguration = new Configuration.ReporterConfiguration(config().getBoolean("JAEGER_REPORTER_LOG_SPANS"), config().getString("JAEGER_AGENT_HOST"), config().getInteger("JAEGER_AGENT_PORT"), config().getInteger("JAEGER_REPORTER_FLUSH_INTERVAL"), config().getInteger("JAEGER_REPORTER_MAX_QUEUE_SIZE"));
        Configuration configuration = new Configuration(config().getString("JAEGER_SERVICE_NAME"), samplerConfiguration, reporterConfiguration);
        TracingHandler tracingHandler = new TracingHandler(configuration.getTracer());
        router.route().order(-1).handler(tracingHandler).failureHandler(tracingHandler);
    }

    private void query(RoutingContext routingContext) {
        getActiveSpan(routingContext).setTag("Operation", "Look Up Flights");

        HttpRequest<Buffer> httpRequest = webClient
            .getAbs(config().getString("service.airports.baseUrl") + "/airports");
        HttpServerResponse response = routingContext.response();
        traceOutgoingCall(routingContext, httpRequest);
        httpRequest.send(asyncResult -> {
            if (asyncResult.succeeded()) {
                logger.fine("Got code " + asyncResult.result().statusCode());
                DeliveryOptions options = new DeliveryOptions();
                options.addHeader("date", routingContext.request().getParam("date"));
                options.addHeader("origin", routingContext.request().getParam("origin"));
                options.addHeader("destination", routingContext.request().getParam("destination"));
                vertx.eventBus().<String>send(FlightSchedulingService.class.getName(),
                    asyncResult.result().bodyAsString(), options, reply -> {
                    if (reply.succeeded()) {
                        response.putHeader("Content-Type", "application/json; charset=utf-8");
                        response.end(reply.result().body());
                    } else {
                        handleExceptionResponse(response, reply.cause());
                    }
                });
            } else {
                handleExceptionResponse(response, asyncResult.cause());
            }
        });
    }

    private void handleExceptionResponse(HttpServerResponse response, Throwable throwable) {
        logger.log(Level.SEVERE, "Failed to query flights", throwable);
        if (!response.ended())
            response.setStatusCode(500).setStatusMessage(throwable.getMessage()).end();
    }

    private void traceOutgoingCall(RoutingContext routingContext, HttpRequest<Buffer> httpRequest) {
        Object object = routingContext.get(CURRENT_SPAN);
        if (object instanceof io.opentracing.Span) {
            io.opentracing.Span span = (io.opentracing.Span) object;

            Configuration.SamplerConfiguration samplerConfiguration = new Configuration.SamplerConfiguration(config().getString("JAEGER_SAMPLER_TYPE"), config().getDouble("JAEGER_SAMPLER_PARAM"), config().getString("JAEGER_SAMPLER_MANAGER_HOST_PORT"));
            Configuration.ReporterConfiguration reporterConfiguration = new Configuration.ReporterConfiguration(config().getBoolean("JAEGER_REPORTER_LOG_SPANS"), config().getString("JAEGER_AGENT_HOST"), config().getInteger("JAEGER_AGENT_PORT"), config().getInteger("JAEGER_REPORTER_FLUSH_INTERVAL"), config().getInteger("JAEGER_REPORTER_MAX_QUEUE_SIZE"));
            Configuration configuration = new Configuration(config().getString("JAEGER_SERVICE_NAME"), samplerConfiguration, reporterConfiguration);
            io.opentracing.Tracer tracer = configuration.getTracer();
            Map<String, String> headerAdditions = new HashMap<>();
            tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new TextMapInjectAdapter(headerAdditions));
            httpRequest.headers().addAll(headerAdditions);
        }
    }

    private Span getActiveSpan(RoutingContext routingContext) {
        return routingContext.get(CURRENT_SPAN);
    }
}
