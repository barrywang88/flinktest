package com.barry.flink;

import com.barry.flink.source.TaxiFareGenerator;
import com.barry.flink.domain.TaxiFare;
import com.barry.flink.domain.TaxiRide;
import com.barry.flink.source.TaxiFareGenerator;
import com.barry.flink.source.TaxiRideGenerator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Stateful Enrichment" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 *
 */
public class RidesAndFaresSolution extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        // Set up streaming execution environment, including Web UI and REST endpoint.
        // Checkpointing isn't needed for the RidesAndFares exercise; this setup is for
        // using the State Processor API.

        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///C:\\data\\bak/savepoints");
        conf.setString("state.checkpoints.dir", "file:///C:\\data\\bak/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(ExerciseBase.parallelism);

        env.enableCheckpointing(10000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideGenerator()))
                .filter((TaxiRide ride) -> ride.isStart)
                .keyBy((TaxiRide ride) -> ride.rideId);

        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareGenerator()))
                .keyBy((TaxiFare fare) -> fare.rideId);

        // Set a UID on the stateful flatmap operator so we can read its state using the State Processor API.
        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
                .connect(fares)
                .flatMap(new EnrichmentFunction())
                .uid("enrichment");

        printOrTest(enrichedRides);

        env.execute("Join Rides with Fares (java RichCoFlatMap)");
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // keyed, managed state
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(Tuple2.of(ride, fare));
            } else {
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(Tuple2.of(ride, fare));
            } else {
                fareState.update(fare);
            }
        }
    }
}