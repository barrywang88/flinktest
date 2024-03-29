package com.barry.flink;

import com.barry.flink.domain.TaxiRide;
import com.barry.flink.source.TaxiRideGenerator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Solution to the "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
public class LongRidesSolution extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> longRides = rides
                .keyBy((TaxiRide ride) -> ride.rideId)
                .process(new MatchFunction());

        printOrTest(longRides);

        env.execute("Long Taxi Rides");
    }

    private static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {
        // keyed, managed state
        // holds an END event if the ride has ended, otherwise a START event
        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> startDescriptor =
                    new ValueStateDescriptor<>("saved ride", TaxiRide.class);
            rideState = getRuntimeContext().getState(startDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
            TimerService timerService = context.timerService();

            if (ride.isStart) {
                // the matching END might have arrived first; don't overwrite it
                if (rideState.value() == null) {
                    rideState.update(ride);
                }
            } else {
                rideState.update(ride);
            }

            timerService.registerEventTimeTimer(ride.getEventTime() + 120 * 60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
            TaxiRide savedRide = rideState.value();
            if (savedRide != null && savedRide.isStart) {
                out.collect(savedRide);
            }
            rideState.clear();
        }
    }

}