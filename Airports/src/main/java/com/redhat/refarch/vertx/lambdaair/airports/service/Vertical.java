package com.redhat.refarch.vertx.lambdaair.airports.service;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.logging.Logger;

import com.redhat.refarch.vertx.lambdaair.airports.model.Airport;
import com.uber.jaeger.Configuration;
import com.uber.jaeger.Span;
import io.opentracing.contrib.vertx.ext.web.TracingHandler;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class Vertical extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        try {
            AirportsService.loadAirports();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void start(Future<Void> startFuture) {
        ConfigStoreOptions store = new ConfigStoreOptions();
        store.setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", "app-config.yml"));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));
        retriever.getConfig(result -> startWithConfig(startFuture, result));
    }

    private void startWithConfig(Future<Void> startFuture, AsyncResult<JsonObject> configResult) {
        if (configResult.failed()) {
            startFuture.fail(configResult.cause());
        }
        Router router = Router.router(vertx);
        router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
        router.get("/airports").handler(this::getAirports);
        router.get("/airports/:airport").handler(this::getAirport);
        setupTracing(router, configResult.result());
        HttpServer httpServer = vertx.createHttpServer();
        httpServer.requestHandler(router::accept);
        int port = configResult.result().getInteger("http.port", 8080);
        httpServer.listen(port, result -> {
            if (result.succeeded()) {
                startFuture.succeeded();
                logger.info("Running http server on port " + result.result().actualPort());
            } else {
                startFuture.fail(result.cause());
            }
        });
    }

    private void setupTracing(Router router, JsonObject config) {
        Configuration.SamplerConfiguration samplerConfiguration =
                new Configuration.SamplerConfiguration(
                        config.getString("JAEGER_SAMPLER_TYPE"),
                        config.getDouble("JAEGER_SAMPLER_PARAM"),
                        config.getString("JAEGER_SAMPLER_MANAGER_HOST_PORT"));

        Configuration.ReporterConfiguration reporterConfiguration =
                new Configuration.ReporterConfiguration(
                        config.getBoolean("JAEGER_REPORTER_LOG_SPANS"),
                        config.getString("JAEGER_AGENT_HOST"),
                        config.getInteger("JAEGER_AGENT_PORT"),
                        config.getInteger("JAEGER_REPORTER_FLUSH_INTERVAL"),
                        config.getInteger("JAEGER_REPORTER_MAX_QUEUE_SIZE"));

        Configuration configuration = new Configuration(
                config.getString("JAEGER_SERVICE_NAME"),
                samplerConfiguration, reporterConfiguration);

        TracingHandler tracingHandler = new TracingHandler(configuration.getTracer());
        router.route().order(-1).handler(tracingHandler).failureHandler(tracingHandler);
    }

    private void getAirports(RoutingContext routingContext) {
        getActiveSpan(routingContext).setTag("Operation", "Look Up Airports");
        HttpServerResponse response = routingContext.response();
        response.putHeader("Content-Type", "application/json; charset=utf-8");

        String filter = routingContext.request().getParam("filter");
        logger.fine("Filter is " + filter);
        Collection<Airport> airports;
        if (filter == null || filter.isEmpty()) {
            airports = AirportsService.getAirports();
        } else {
            airports = AirportsService.filter(filter);
        }
        response.end(Json.encode(airports));
    }

    private void getAirport(RoutingContext routingContext) {
        getActiveSpan(routingContext).setTag("Operation", "Look Up Single Airport");
        String code = routingContext.request().getParam("airport");
        Airport airport = AirportsService.getAirport(code.toUpperCase(Locale.US));
        logger.fine("Got airport " + airport);
        HttpServerResponse response = routingContext.response();
        response.putHeader("Content-Type", "application/json; charset=utf-8");
        if (airport == null) {
            response.setStatusCode(204).end();
        } else {
            response.end(Json.encode(airport));
        }
    }

    private Span getActiveSpan(RoutingContext routingContext) {
        return routingContext.get(TracingHandler.CURRENT_SPAN);
    }
}
