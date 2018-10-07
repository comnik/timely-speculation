//! https://www.youtube.com/watch?v=3UfZN59Nsk8

extern crate timely;

use std::time;

use timely::{Configuration, PartialOrder};
use timely::dataflow::operators::{Input, UnorderedInput, Inspect, Probe, Map, Delay};
use timely::dataflow::operators::aggregation::{Aggregate};
use timely::progress::timestamp::{RootTimestamp};

mod custom_aggregate;
use custom_aggregate::{CustomAggregate};

fn main () {
    timely::execute(Configuration::Thread, |worker| {

        let ((mut input, mut cap), mut spec, probe) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            let (spec, spec_stream) = scope.new_input();
            
            let probe = stream
                .inspect_batch(|t, x| println!("IN {:?} -> {:?}", t, x))
                // fixed size windows, each 20 units in size
                // .delay(|_data, time| RootTimestamp::new(((time.inner / 20) + 1) * 20))
                .map(|x| (0, x))
                .aggregate_speculative(
                    &spec_stream,
                    |_key, val, agg| { *agg += val; },
                    |_key, agg: u64| agg,
                    |key| *key as u64
                )
                .inspect_batch(|t, x| println!("OUT {:?} -> {:?}", t, x))
                .probe();

            (input, spec, probe)
        });
       
        // (event_time, processing_time, score)
        let mut data: Vec<(usize, usize, u64)> = vec![
            (5, 52, 5), (22, 56, 7), (36, 62, 3),
            (38, 63, 4), (45, 66, 3), (30, 71, 8),
            (68, 73, 3), (15, 85, 9), (75, 87, 8), (78, 90, 1)
        ];
        let experiment_time = time::Instant::now();
        
        for (event_time, processing_time, score) in data.drain(..) {
            while ((experiment_time.elapsed().as_secs() as usize) * 10) < processing_time {
                worker.step();
            }

            let t = RootTimestamp::new(event_time);

            if !cap.time().less_equal(&t) {
                // Discard late arrival.
                println!("DISCARDING {}", score);
            } else {
                let mut session = input.session(cap.delayed(&RootTimestamp::new(((event_time / 20) + 1) * 20)));//cap.delayed(&t)
                session.give(score);
                drop(session);

                // ideal watermark
                // cap = cap.delayed(&RootTimestamp::new(event_time + 1));

                if spec.time().less_equal(&t) {
                    spec.send(());
                    // spec.advance_to(event_time);
                    spec.advance_to(((event_time / 20) + 1) * 20);
                }
            }
        }

        drop(cap);
        drop(spec);

        while !probe.done() { worker.step(); }

    }).unwrap();
}
