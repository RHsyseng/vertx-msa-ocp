package com.redhat.refarch.vertx.lambdaair.sales.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import au.com.bytecode.opencsv.CSVReader;
import com.redhat.refarch.vertx.lambdaair.sales.model.Flight;
import com.redhat.refarch.vertx.lambdaair.sales.model.FlightSegment;
import com.redhat.refarch.vertx.lambdaair.sales.model.Itinerary;
import com.redhat.refarch.vertx.lambdaair.sales.model.Pricing;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;

public class SalesTicketingService extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(SalesTicketingService.class.getName());
    private static int counter = 1;

    private static Map<Integer, Pricing> pricingMap = new HashMap<>();

    private static final double extraHopDiscount = 0.8;

    private static final double layoverDiscount = 0.9;

    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(SalesTicketingService.class.getName(), SalesTicketingService::price);
        loadPricing().setHandler(startFuture.completer());
    }

    private static Future<Void> loadPricing() {
        Future<Void> future = Future.future();
        try {
            if (pricingMap.isEmpty()) {
                InputStream inputStream = SalesTicketingService.class.getResourceAsStream("/pricing.csv");
                CSVReader reader = new CSVReader(new InputStreamReader(inputStream), '\t');
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    Pricing pricing = new Pricing();
                    pricing.flightNumber = Integer.parseInt(nextLine[0]);
                    pricing.basePrice = Integer.parseInt(nextLine[1]);
                    pricing.availability = new int[nextLine.length - 2];
                    for (int index = 2; index < nextLine.length; index++) {
                        pricing.availability[index - 2] = Integer.parseInt(nextLine[index]);
                    }
                    pricingMap.put(pricing.flightNumber, pricing);
                }
                logger.info("SalesTicketingService successfully initialized");
            }
            future.complete();
        } catch (IOException e) {
            future.fail(e);
        }
        return future;
    }

    private static void price(Message<String> message) {
        try {
            logger.info("Wait " + (counter++));
            Thread.sleep(3000);
            logger.info("Waited");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Flight flight = Json.decodeValue(message.body(), Flight.class);
        int dateOffset = getDateOffset(flight.getSegments()[0].getDepartureTime());
        int price = 0;
        Instant arrival = null;
        for (FlightSegment flightSegment : flight.getSegments()) {
            Pricing pricing = pricingMap.get(flightSegment.getFlightNumber());
            int segmentPrice = pricing.basePrice;
            segmentPrice *= getAvailabilityDiscount(dateOffset, pricing.availability[dateOffset - 1]);
            if (price == 0) {
                price = segmentPrice;
            } else {
                int hopPrice = segmentPrice;
                int layover = (int) Duration.between(arrival, flightSegment.getDepartureTime()).toHours() - 1;
                if (layover > 0) {
                    //Apply a discount for every extra hour of layover:
                    hopPrice *= layover * layoverDiscount;
                }
                //Add to total price so far:
                price += hopPrice;
                //Apply another discount on top of total for extra hop:
                price *= extraHopDiscount;
            }
            arrival = flightSegment.getArrivalTime();
        }
        Itinerary itinerary = new Itinerary(flight);
        logger.fine("Priced ticket at " + price);
        itinerary.setPrice(price);
        message.reply(Json.encode(itinerary));
    }

    private static double getAvailabilityDiscount(int dateOffset, int availability) {
        if (dateOffset <= 3) {
            if (availability > 60) {
                return 0.7;
            } else if (availability > 50) {
                return 0.8;
            } else {
                return 1;
            }
        } else if (dateOffset <= 7) {
            if (availability > 70) {
                return 0.75;
            } else {
                return 1;
            }
        } else if (dateOffset <= 21) {
            if (availability > 90) {
                return 0.6;
            } else if (availability > 80) {
                return 0.8;
            } else {
                return 1;
            }
        } else {
            if (availability > 90) {
                return 0.9;
            } else {
                return 1;
            }
        }
    }

    private static int getDateOffset(Instant instant) {
        int dateOffset = (int) Duration.between(Instant.now(), instant).toDays();
        if (dateOffset < 1) {
            dateOffset = 1;
        } else if (dateOffset > 60) {
            dateOffset = 60;
        }
        return dateOffset;
    }
}