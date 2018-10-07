extern crate timely;

use std::time;

use timely::{Configuration, PartialOrder};
use timely::dataflow::operators::{Input, UnorderedInput, Inspect, Probe, Map, Delay};
use timely::progress::timestamp::{RootTimestamp};

mod custom_aggregate;
use custom_aggregate::{CustomAggregate};

fn main () {
    timely::execute(Configuration::Thread, |worker| {

        let fixed_window = |size: usize| { move |t: usize| ((t / size) + 1) * size };
        let window = fixed_window(20);
        
        let ((mut input, mut cap), mut spec, probe) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            let (spec, spec_stream) = scope.new_input();
            
            let probe = stream
                .inspect_batch(|t, x| println!("IN {:?} -> {:?}", t, x))
                // .delay(|_data, time| RootTimestamp::new(window(time.inner)))
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
            ( 5, 52, 5),
            (22, 56, 7),
            (36, 62, 3),
            (38, 63, 4),
            (45, 66, 3),
            (30, 71, 8),
            (68, 73, 3),
            (15, 85, 9),
            (75, 87, 8),
            (78, 90, 1)
        ];
        let experiment_time = time::Instant::now();
        let mut windows: Vec<usize> = vec![20, 40, 60, 80];
        
        for (event_time, processing_time, score) in data.drain(..) {

            // simulate out-of-order arrival (1s real time = 10 units)
            while ((experiment_time.elapsed().as_secs() as usize) * 10) <= processing_time {
                worker.step();
            }
            
            let processing_t = RootTimestamp::new((experiment_time.elapsed().as_secs() as usize) * 10);
            let event_t = RootTimestamp::new(event_time);

            if !cap.time().less_equal(&event_t) {
                // discard late arrivals
                println!("DISCARDING {}", score);
            } else {
                // give score to its event-time window
                let window_t = RootTimestamp::new(window(event_time));
                let mut session = input.session(cap.delayed(&window_t));
                session.give(score);
                drop(session);

                for window in windows.iter() {
                    if ((window + 30) <= processing_t.inner) && (spec.time().inner <= *window) {
                        // trigger speculative results for 
                        spec.send(());
                        spec.advance_to(*window);

                        // ideally, we would never see late arrivals
                        // cap = cap.delayed(&RootTimestamp::new(*window));
                    }
                }
                windows.retain(|t| spec.time().inner <= *t);
            }
        }

        drop(cap);
        drop(spec);

        while !probe.done() { worker.step(); }

    }).unwrap();
}
