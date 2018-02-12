package com.redhat.refarch.vertx.lambdaair.flights.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import au.com.bytecode.opencsv.CSVReader;
import com.redhat.refarch.vertx.lambdaair.flights.model.Airport;
import com.redhat.refarch.vertx.lambdaair.flights.model.Flight;
import com.redhat.refarch.vertx.lambdaair.flights.model.FlightSchedule;
import com.redhat.refarch.vertx.lambdaair.flights.model.FlightSegment;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;

public class FlightSchedulingService extends AbstractVerticle {
    private static Logger logger = Logger.getLogger(Vertical.class.getName());

    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static Map<String, List<FlightSchedule>> departingFlights = new HashMap<>();

    private static Map<String, List<FlightSchedule>> arrivingFlights = new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(FlightSchedulingService.class.getName(), FlightSchedulingService::query);
        loadFlightSchedule().setHandler(startFuture.completer());
    }

    private static Future<Void> loadFlightSchedule() {
        Future<Void> future = Future.future();
        try {
            if (departingFlights.isEmpty()) {
                InputStream inputStream = FlightSchedulingService.class.getResourceAsStream("/flights.csv");
                CSVReader reader = new CSVReader(new InputStreamReader(inputStream), '\t');
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    FlightSchedule flightSchedule = new FlightSchedule();
                    flightSchedule.setFlightNumber(nextLine[1]);
                    flightSchedule.setDepartureAirport(nextLine[2]);
                    flightSchedule.setDepartureTime(LocalTime.parse(nextLine[3], formatter));
                    flightSchedule.setArrivalAirport(nextLine[4]);
                    flightSchedule.setArrivalTime(LocalTime.parse(nextLine[5], formatter));
                    departingFlights.computeIfAbsent(flightSchedule.getDepartureAirport(), s -> new ArrayList<>()).add(flightSchedule);
                    arrivingFlights.computeIfAbsent(flightSchedule.getArrivalAirport(), s -> new ArrayList<>()).add(flightSchedule);
                }
            }
            future.complete();
        } catch (IOException e) {
            future.fail(e);
        }
        return future;
    }

    private static void query(Message<String> message) {
        Airport[] airportArray = Json.decodeValue(message.body(), Airport[].class);
        Map<String, Airport> airports = Arrays.stream(airportArray).collect(Collectors.toMap(Airport::getCode, airport -> airport));
        String date = message.headers().get("date");
        String origin = message.headers().get("origin");
        String destination = message.headers().get("destination");

        LocalDate travelDate = LocalDate.parse(date, dateFormatter);
        logger.fine(origin + " => " + destination + " on " + travelDate);
        List<FlightSchedule[]> routes = FlightSchedulingService.getRoutes(airports, origin, destination);
        List<Flight> flights = new ArrayList<>();
        for (FlightSchedule[] route : routes) {
            FlightSegment[] segments = new FlightSegment[route.length];
            for (int index = 0; index < segments.length; index++) {
                segments[index] = new FlightSegment();
                segments[index].setFlightNumber(Integer.parseInt(route[index].getFlightNumber()));
                segments[index].setDepartureAirport(route[index].getDepartureAirport());
                segments[index].setArrivalAirport(route[index].getArrivalAirport());
                //For now assume all travel time is for the requested date and not +1.
                segments[index].setDepartureTime(getInstant(travelDate, route[index].getDepartureTime(), airports.get(route[index].getDepartureAirport()).getZoneId()));
                segments[index].setArrivalTime(getInstant(travelDate, route[index].getArrivalTime(), airports.get(route[index].getArrivalAirport()).getZoneId()));
            }
            //Fix the timestamp when date is the next morning
            Instant previousTimestamp = segments[0].getDepartureTime();
            for (FlightSegment segment : segments) {
                if (previousTimestamp.isAfter(segment.getDepartureTime())) {
                    segment.setDepartureTime(segment.getDepartureTime().plus(1, ChronoUnit.DAYS));
                }
                previousTimestamp = segment.getDepartureTime();
                if (previousTimestamp.isAfter(segment.getArrivalTime())) {
                    segment.setArrivalTime(segment.getArrivalTime().plus(1, ChronoUnit.DAYS));
                }
                previousTimestamp = segment.getArrivalTime();
            }
            Flight flight = new Flight();
            flight.setSegments(segments);
            flights.add(flight);
        }
        for (Flight flight : flights) {
            if (flight.getSegments().length == 1) {
                logger.fine("Nonstop:\t" + flight.getSegments()[0]);
            } else {
                logger.fine("One stop\n\t" + flight.getSegments()[0] + "\n\t" + flight.getSegments()[1]);
            }
        }
        message.reply(Json.encode(flights));
    }

    private static List<FlightSchedule[]> getRoutes(Map<String, Airport> airports, String origin, String destination) {
        List<FlightSchedule[]> routes = new ArrayList<>();
        List<FlightSchedule> departing = departingFlights.get(origin);
        List<FlightSchedule> arriving = arrivingFlights.get(destination);
        if (departing != null && arriving != null) {
            List<FlightSchedule> firstLegOptions = new ArrayList<>();
            for (FlightSchedule flightSchedule : departing) {
                if (flightSchedule.getArrivalAirport().equals(destination)) {
                    //Found a non-stop!
                    routes.add(new FlightSchedule[]{flightSchedule});
                } else {
                    firstLegOptions.add(flightSchedule);
                }
            }
            for (FlightSchedule firstLeg : firstLegOptions) {
                for (FlightSchedule secondLeg : arriving) {
                    if (connectingFlights(airports, firstLeg, secondLeg)) {
                        routes.add(new FlightSchedule[]{firstLeg, secondLeg});
                    }
                }
            }
        }
        return routes;
    }

    private static boolean connectingFlights(Map<String, Airport> airports, FlightSchedule firstLeg, FlightSchedule secondLeg) {
        if (firstLeg.getArrivalAirport().equals(secondLeg.getDepartureAirport())) {
            Duration layover = getPositiveDuration(firstLeg.getArrivalTime(), secondLeg.getDepartureTime());
            Duration minLayover = Duration.ofMinutes(30);
            Duration maxLayover = getPositiveDuration(firstLeg.getDepartureTime(), airports.get(firstLeg.getDepartureAirport()).getZoneId(), firstLeg.getArrivalTime(), airports.get(firstLeg.getArrivalAirport()).getZoneId());
            return layover.compareTo(minLayover) >= 0 && layover.compareTo(maxLayover) <= 0;
        } else {
            return false;
        }
    }

    private static Duration getPositiveDuration(LocalTime startTime, LocalTime endTime) {
        return getPositiveDuration(startTime, ZoneId.systemDefault(), endTime, ZoneId.systemDefault());
    }

    private static Duration getPositiveDuration(LocalTime startTime, ZoneId startZone, LocalTime endTime, ZoneId endZone) {
        ZonedDateTime start = ZonedDateTime.of(LocalDate.now(), startTime, startZone);
        ZonedDateTime end = ZonedDateTime.of(LocalDate.now(), endTime, endZone);
        if (end.isBefore(start)) {
            end = end.plus(1, ChronoUnit.DAYS);
        }
        return Duration.between(start, end);
    }

    private static Instant getInstant(LocalDate travelDate, LocalTime localTime, ZoneId zoneId) {
        return ZonedDateTime.of(travelDate, localTime, zoneId).toInstant();
    }
}
