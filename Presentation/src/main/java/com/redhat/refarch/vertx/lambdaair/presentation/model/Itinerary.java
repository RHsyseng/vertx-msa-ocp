package com.redhat.refarch.vertx.lambdaair.presentation.model;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Itinerary {
    private static NumberFormat priceFormat = new DecimalFormat("$#,##0");

    private Flight[] flights;

    private int price;

    public Itinerary() {
    }

    public Itinerary(Flight... flights) {
        this.flights = flights;
    }

    public Flight[] getFlights() {
        return flights;
    }

    public void setFlights(Flight[] flights) {
        this.flights = flights;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getDuration() {
        return Arrays.stream(flights).mapToInt(Flight::getDuration).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Itinerary itinerary = (Itinerary) o;

        if (Double.compare(itinerary.price, price) != 0) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(flights, itinerary.flights);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(price);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + Arrays.hashCode(flights);
        return result;
    }

    @Override
    public String toString() {
        return "Itinerary{" + "price=" + price + ", flights=" + Arrays.toString(flights) + '}';
    }

    public static Comparator<Itinerary> priceComparator = Comparator.comparingInt(Itinerary::getPrice);

    public static Comparator<Itinerary> durationComparator = Comparator.comparingInt(Itinerary::getDuration);
}