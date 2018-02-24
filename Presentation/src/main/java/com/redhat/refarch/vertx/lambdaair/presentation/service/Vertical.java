package com.redhat.refarch.vertx.lambdaair.presentation.service;

import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.redhat.refarch.vertx.lambdaair.presentation.model.Airport;
import com.redhat.refarch.vertx.lambdaair.presentation.model.Flight;
import com.redhat.refarch.vertx.lambdaair.presentation.model.FlightSegment;
import com.redhat.refarch.vertx.lambdaair.presentation.model.Itinerary;
import com.uber.jaeger.Configuration;
import com.uber.jaeger.Span;
import io.opentracing.SpanContext;
import io.opentracing.contrib.vertx.ext.web.TracingHandler;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.rxjava.circuitbreaker.CircuitBreaker;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Future;
import io.vertx.rxjava.core.MultiMap;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.client.HttpRequest;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import org.apache.http.client.utils.URIBuilder;
import rx.Observable;
import rx.Single;

public class Vertical extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());

    private static int counter = 1;

    private Configuration configuration;

    private WebClient webClient;
    private CircuitBreaker circuitBreaker;

    @Override
    public void start(io.vertx.core.Future<Void> startFuture) {
        Json.mapper.registerModule(new JavaTimeModule());
        ConfigStoreOptions store = new ConfigStoreOptions();
        store.setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", "app-config.yml"));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store));
        retriever.getConfig(result -> startWithConfig(startFuture, result));
    }

    private void startWithConfig(io.vertx.core.Future<Void> startFuture, AsyncResult<JsonObject> configResult) {
        if (configResult.failed()) {
            throw new IllegalStateException(configResult.cause());
        }
        mergeIn(null, configResult.result().getMap());

        WebClientOptions options = new WebClientOptions();
        int threadPoolSize = config().getInteger("pricing.pool.size");
        logger.fine("Will price in batches of " + threadPoolSize);
        options.setMaxPoolSize(threadPoolSize);
        webClient = WebClient.create(vertx, options);

        CircuitBreakerOptions circuitBreakerOptions = new CircuitBreakerOptions()
            .setMaxFailures(config().getInteger("pricing.failure.max", 3))
            .setTimeout(config().getInteger("pricing.failure.timeout", 1000))
            .setFallbackOnFailure(config().getBoolean("pricing.failure.fallback", false))
            .setResetTimeout(config().getInteger("pricing.failure.reset", 5000));
        circuitBreaker = CircuitBreaker.create("PricingCall", vertx, circuitBreakerOptions);

        Configuration.SamplerConfiguration samplerConfiguration = new Configuration.SamplerConfiguration(config().getString("JAEGER_SAMPLER_TYPE"), config().getDouble("JAEGER_SAMPLER_PARAM"), config().getString("JAEGER_SAMPLER_MANAGER_HOST_PORT"));
        Configuration.ReporterConfiguration reporterConfiguration = new Configuration.ReporterConfiguration(config().getBoolean("JAEGER_REPORTER_LOG_SPANS"), config().getString("JAEGER_AGENT_HOST"), config().getInteger("JAEGER_AGENT_PORT"), config().getInteger("JAEGER_REPORTER_FLUSH_INTERVAL"), config().getInteger("JAEGER_REPORTER_MAX_QUEUE_SIZE"));
        configuration = new Configuration(config().getString("JAEGER_SERVICE_NAME"), samplerConfiguration, reporterConfiguration);

        Router router = Router.router(vertx);
        router.get("/health").handler(routingContext -> routingContext.response().end("OK"));
        router.get("/gateway/airportCodes").handler(this::getAirportCodes);
        router.get("/gateway/query").handler(this::query);
        router.get("/*").handler(StaticHandler.create("webapp"));
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
        Handler<RoutingContext> wrapperHandler = routingContext -> tracingHandler.handle(routingContext.getDelegate());
        router.route().order(-1).handler(wrapperHandler).failureHandler(wrapperHandler);
    }

    private void getAirportCodes(RoutingContext routingContext) {
        getActiveSpan(routingContext).setTag("Operation", "Look Up Airport Codes");
        getAirports(routingContext).subscribe(airports -> {
            List<String> airportDescriptors = Arrays.stream(airports).map(Vertical::mapAirportCodes).collect(Collectors.toList());
            HttpServerResponse response = routingContext.response();
            response.putHeader("Content-Type", "application/json; charset=utf-8");
            response.end(Json.encode(airportDescriptors));
        }, throwable -> handleExceptionResponse(routingContext, throwable));
    }

    private static String mapAirportCodes(Airport airport) {
        return airport.getCode() + "\t" + airport.getCity() + " - " + airport.getName();
    }

    private void query(RoutingContext routingContext) {
        Span querySpan = getActiveSpan(routingContext);
        querySpan.setTag("Operation", "Itinerary Query");
        querySpan.setBaggageItem("forwarded-for", routingContext.request().getHeader("x-forwarded-for"));
        MultiMap queryParams = routingContext.request().params();
        Observable<Airport[]> airportsObservable = getAirports(routingContext);
        Observable<Flight[]> flightsObservable = airportsObservable.concatMap(airports -> getFlights(routingContext, airports, "departureDate", "origin", "destination"));
        Observable<List<Itinerary>> itinerariesObservable = flightsObservable.concatMap(flights -> priceFlights(routingContext, flights));
        if (queryParams.get("returnDate") != null) {
            Observable<Flight[]> returnFlightsObservable = airportsObservable.concatMap(airports -> getFlights(routingContext, airports, "returnDate", "destination", "origin"));
            Observable<List<Itinerary>> returnItinerariesObservable = returnFlightsObservable.concatMap(flights -> priceFlights(routingContext, flights));
            itinerariesObservable = itinerariesObservable.zipWith(returnItinerariesObservable, (departureItineraries, returnItineraries) -> {
                List<Itinerary> itineraries = new ArrayList<>();
                for (Itinerary departingItinerary : departureItineraries) {
                    for (Itinerary returnItinerary : returnItineraries) {
                        Itinerary itinerary = new Itinerary(departingItinerary.getFlights()[0], returnItinerary.getFlights()[0]);
                        itinerary.setPrice(departingItinerary.getPrice() + returnItinerary.getPrice());
                        itineraries.add(itinerary);
                    }
                }
                return itineraries;
            });
        }
        itinerariesObservable.subscribe(itineraries -> {
            itineraries.sort(Itinerary.durationComparator);
            itineraries.sort(Itinerary.priceComparator);
            logger.fine("Returning " + itineraries.size() + " flights");
            routingContext.response().putHeader("Content-Type", "application/json; charset=utf-8");
            routingContext.response().end(Json.encode(itineraries));
        }, throwable -> handleExceptionResponse(routingContext, throwable));
    }

    private Observable<Airport[]> getAirports(RoutingContext routingContext) {
        String uri = config().getString("service.airports.baseUrl") + "/airports";
        HttpRequest<Buffer> httpRequest = webClient.getAbs(uri);
        traceOutgoingCall(routingContext, httpRequest);
        return httpRequest.rxSend().map(httpResponse -> {
            if (httpResponse.statusCode() < 300) {
                return httpResponse.bodyAsJson(Airport[].class);
            } else {
                throw new RuntimeException("Airport call failed with " + httpResponse.statusCode() + ": " + httpResponse.statusMessage());
            }
        }).toObservable();
    }

    private Observable<Flight[]> getFlights(RoutingContext routingContext, Airport[] airports, String date, String origin, String destination) {
        try {
            URIBuilder uriBuilder = new URIBuilder(config().getString("service.flights.baseUrl") + "/query");
            MultiMap queryParams = routingContext.request().params();
            uriBuilder.addParameter("date", queryParams.get(date));
            uriBuilder.addParameter("origin", queryParams.get(origin));
            uriBuilder.addParameter("destination", queryParams.get(destination));
            HttpRequest<Buffer> httpRequest = webClient.getAbs(uriBuilder.toString());
            traceOutgoingCall(routingContext, httpRequest);
            return httpRequest.rxSend().map(httpResponse -> {
                if (httpResponse.statusCode() < 300) {
                    Flight[] flights = httpResponse.bodyAsJson(Flight[].class);
                    Map<String, Airport> airportMap = Arrays.stream(airports).collect(Collectors.toMap(Airport::getCode, airport -> airport));
                    populateFormattedTimes(flights, airportMap);
                    return flights;
                } else {
                    throw new RuntimeException("Flight call failed with " + httpResponse.statusCode() + ": " + httpResponse.statusMessage());
                }
            }).toObservable();
        } catch (URISyntaxException e) {
            return Single.<Flight[]>error(e).toObservable();
        }
    }

    private Observable<List<Itinerary>> priceFlights(RoutingContext routingContext, Flight[] flights) {
        HttpRequest<Buffer> request = webClient.postAbs(config().getString("service.sales.baseUrl") + "/price");
        traceOutgoingCall(routingContext, request);
        List<Observable<Itinerary>> itineraryObservables = new ArrayList<>();
        for (Flight flight : flights) {
            Handler<Future<Itinerary>> priceFlightHandler = future -> {
                logger.info("Calling " + (counter++));
                request.rxSendJson(flight).subscribe(httpResponse -> {
                    logger.info("Response");
                    future.complete(httpResponse.bodyAsJson(Itinerary.class));
                }, future::fail);
            };
            Function<Throwable, Itinerary> priceFlightFailback = throwable -> {
                logger.log(Level.WARNING, "Fallback while obtaining price for " + flight, throwable);
                return new Itinerary(flight);
            };
            Observable<Itinerary> observable = circuitBreaker.rxExecuteCommandWithFallback(priceFlightHandler, priceFlightFailback).toObservable();
            itineraryObservables.add(observable);
        }
        return Observable.zip(itineraryObservables, objects -> {
            List<Itinerary> pricedItineraries = new ArrayList<>();
            for (Object object : objects) {
                pricedItineraries.add((Itinerary) object);
            }
            return pricedItineraries;
        });
    }

    private void traceOutgoingCall(RoutingContext routingContext, HttpRequest<Buffer> httpRequest) {
        SpanContext spanContext = getActiveSpan(routingContext).context();
        Map<String, String> headerAdditions = new HashMap<>();
        configuration.getTracer().inject(spanContext, Format.Builtin.HTTP_HEADERS, new TextMapInjectAdapter(headerAdditions));
        headerAdditions.forEach(httpRequest.headers()::add);
    }

    private static void populateFormattedTimes(Flight[] flights, Map<String, Airport> airports) {
        for (Flight flight : flights) {
            for (FlightSegment segment : flight.getSegments()) {
                segment.setFormattedDepartureTime(getFormattedTime(segment.getDepartureTime(), airports.get(segment.getDepartureAirport())));
                segment.setFormattedArrivalTime(getFormattedTime(segment.getArrivalTime(), airports.get(segment.getArrivalAirport())));
            }
        }
    }

    private static String getFormattedTime(Instant departureTime, Airport airport) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("h:mma");
        formatter = formatter.withLocale(Locale.US);
        formatter = formatter.withZone(ZoneId.of(airport.getZoneId()));
        return formatter.format(departureTime);
    }

    private Span getActiveSpan(RoutingContext routingContext) {
        return routingContext.get(TracingHandler.CURRENT_SPAN);
    }

    private void handleExceptionResponse(RoutingContext routingContext, Throwable throwable) {
        if (!routingContext.response().ended())
            routingContext.response().setStatusCode(500).setStatusMessage(throwable.getMessage()).end();
    }
}
