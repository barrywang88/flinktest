package com.barry.flink.domain;

import com.barry.flink.utils.DataGenerator;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A TaxiFare has payment information about a taxi ride.
 *
 * <p>It has these fields in common with the TaxiRides
 * - the rideId
 * - the taxiId
 * - the driverId
 * - the startTime
 *
 * <p>It also includes
 * - the paymentType
 * - the tip
 * - the tolls
 * - the totalFare
 */
public class TaxiFare implements Serializable {

    /**
     * Creates a TaxiFare with now as the start time.
     */
    public TaxiFare() {
        this.startTime = Instant.now().plusMillis(TimeUnit.HOURS.toMillis(8));
    }

    /**
     * Invents a TaxiFare.
     */
    public TaxiFare(long rideId) {
        DataGenerator g = new DataGenerator(rideId);

        this.rideId = rideId;
        this.taxiId = g.taxiId();
        this.driverId = g.driverId();
        this.startTime = g.startTime();
        this.paymentType = g.paymentType();
        this.tip = g.tip();
        this.tolls = g.tolls();
        this.totalFare = g.totalFare();
    }

    /**
     * Creates a TaxiFare with the given parameters.
     */
    public TaxiFare(long rideId, long taxiId, long driverId, Instant startTime, String paymentType, float tip, float tolls, float totalFare) {
        this.rideId = rideId;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.startTime = startTime;
        this.paymentType = paymentType;
        this.tip = tip;
        this.tolls = tolls;
        this.totalFare = totalFare;
    }

    public long rideId;
    public long taxiId;
    public long driverId;
    public Instant startTime;
    public String paymentType;
    public float tip;
    public float tolls;
    public float totalFare;

    @Override
    public String toString() {

        return "rideId="+rideId + "," +
                taxiId + "," +
                driverId + "," +
                startTime.toString() + "," +
                paymentType + "," +
                tip + "," +
                tolls + "," +
                totalFare;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiFare &&
                this.rideId == ((TaxiFare) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int) this.rideId;
    }

    /**
     * Gets the fare's start time.
     */
    public long getEventTime() {
        return startTime.toEpochMilli();
    }

}
