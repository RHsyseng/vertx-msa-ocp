package com.redhat.refarch.vertx.lambdaair.flights.service;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.uber.jaeger.Configuration;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.vertx.ext.web.TracingHandler;
import io.opentracing.contrib.vertx.ext.web.WebSpanDecorator;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.tag.Tags;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.opentracing.contrib.vertx.ext.web.TracingHandler.CURRENT_SPAN;

public class Verticle extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Verticle.class.getName());

    private Configuration configuration;

    private WebClient webClient;

    @Override
    public void start(Future<Void> startFuture) {
        webClient = WebClient.create(vertx);
        Json.mapper.registerModule(new JavaTimeModule());
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

        Configuration.SamplerConfiguration samplerConfiguration =
                new Configuration.SamplerConfiguration(
                        config().getString("JAEGER_SAMPLER_TYPE"),
                        config().getDouble("JAEGER_SAMPLER_PARAM"),
                        config().getString("JAEGER_SAMPLER_MANAGER_HOST_PORT"));

        Configuration.ReporterConfiguration reporterConfiguration =
                new Configuration.ReporterConfiguration(
                        config().getBoolean("JAEGER_REPORTER_LOG_SPANS"),
                        config().getString("JAEGER_AGENT_HOST"),
                        config().getInteger("JAEGER_AGENT_PORT"),
                        config().getInteger("JAEGER_REPORTER_FLUSH_INTERVAL"),
                        config().getInteger("JAEGER_REPORTER_MAX_QUEUE_SIZE"));

        configuration = new Configuration(
                config().getString("JAEGER_SERVICE_NAME"),
                samplerConfiguration, reporterConfiguration);

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
                httpServer.listen(port, result -> {
                    if (result.succeeded()) {
                        startFuture.succeeded();
                        logger.info("Running http server on port " + result.result().actualPort());
                    } else {
                        startFuture.fail(result.cause());
                    }
                });
            } else {
                startFuture.fail(deployResult.cause());
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
        TracingHandler tracingHandler = new TracingHandler(configuration.getTracer());
        router.route().order(-1).handler(tracingHandler).failureHandler(tracingHandler);
    }

    private void query(RoutingContext routingContext) {
        getActiveSpan(routingContext).setTag("Operation", "Look Up Flights");

        String url = config().getString("service.airports.baseUrl") + "/airports";
        HttpRequest<Buffer> httpRequest = webClient.getAbs(url);
        Span childSpan = traceOutgoingCall(httpRequest, routingContext, HttpMethod.GET, url);
        HttpServerResponse response = routingContext.response();
        httpRequest.send(asyncResult -> {
            if (asyncResult.succeeded()) {
                closeTracingSpan(childSpan, asyncResult.result());
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
                Throwable throwable = asyncResult.cause();
                closeTracingSpan(childSpan, throwable);
                handleExceptionResponse(response, throwable);
            }
        });
    }

    private void handleExceptionResponse(HttpServerResponse response, Throwable throwable) {
        logger.log(Level.SEVERE, "Failed to query flights", throwable);
        if (!response.ended())
            response.setStatusCode(500).setStatusMessage(throwable.getMessage()).end();
    }

    private Span traceOutgoingCall(HttpRequest<Buffer> httpRequest, RoutingContext routingContext, HttpMethod method, String url) {
        Tracer tracer = configuration.getTracer();
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan("Outgoing HTTP request");
        Span activeSpan = getActiveSpan(routingContext);
        spanBuilder = spanBuilder.asChildOf(activeSpan);
        Span span = spanBuilder.start();
        Tags.SPAN_KIND.set(span, Tags.SPAN_KIND_CLIENT);
        Tags.HTTP_URL.set(span, url);
        Tags.HTTP_METHOD.set(span, method.name());

        Map<String, String> headerAdditions = new HashMap<>();
        tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new TextMapInjectAdapter(headerAdditions));
        headerAdditions.forEach(httpRequest.headers()::add);

        return span;
    }

    private void closeTracingSpan(Span span, HttpResponse<Buffer> httpResponse) {
        int status = httpResponse.statusCode();
        Tags.HTTP_STATUS.set(span, status);
        if (status >= 400) {
            Tags.ERROR.set(span, true);
        }
        span.finish();
    }

    private void closeTracingSpan(Span span, Throwable throwable) {
        Tags.ERROR.set(span, true);
        span.log(WebSpanDecorator.StandardTags.exceptionLogs(throwable));
        span.finish();
    }

    private Span getActiveSpan(RoutingContext routingContext) {
        return routingContext.get(CURRENT_SPAN);
    }
}
