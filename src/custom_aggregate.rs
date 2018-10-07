extern crate timely;

use std::fmt::Debug;
use std::hash::Hash;
use std::collections::HashMap;

use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Notificator, FrontierNotificator};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Exchange;

pub trait CustomAggregate<S: Scope, K: Debug+ExchangeData+Hash, V: Debug+ExchangeData> {

    fn aggregate_speculative<R: Data, D: Default+Clone+Debug+'static, F: Fn(&K, V, &mut D)+'static, E: Fn(K, D)->R+'static, H: Fn(&K)->u64+'static>(
        &self,
        spec: &Stream<S, ()>,
        fold: F,
        emit: E,
        hash: H) -> Stream<S, R> where S::Timestamp: Eq;
}

impl<S: Scope, K: Debug+ExchangeData+Hash+Eq, V: Debug+ExchangeData> CustomAggregate<S, K, V> for Stream<S, (K, V)> {

    fn aggregate_speculative<R: Data, D: Default+Clone+Debug+'static, F: Fn(&K, V, &mut D)+'static, E: Fn(K, D)->R+'static, H: Fn(&K)->u64+'static>(
        &self,
        spec: &Stream<S, ()>,
        fold: F,
        emit: E,
        hash: H) -> Stream<S, R> where S::Timestamp: Eq {

        let mut aggregates = HashMap::new();
        let mut vector = Vec::new();
        self.binary_frontier(
            &spec,
            Exchange::new(move |&(ref k, _)| hash(k)),
            Exchange::new(move |_d| 0),
            "CustomAggregate",
            |_cap, _info| {

                let mut notificator = FrontierNotificator::new();
                let mut spec_notif = FrontierNotificator::new();
                let logging = self.scope().logging();
                
                move |input1, input2, output| {

                    let frontiers = &[input1.frontier()];
                    let spec_frontiers = &[input2.frontier()];
                    let notificator = &mut Notificator::new(frontiers, &mut notificator, &logging);
                    let spec_notif = &mut Notificator::new(spec_frontiers, &mut spec_notif, &logging);
                    
                    // read each input, fold into aggregates
                    input1.for_each(|time, data| {
                        data.swap(&mut vector);
                        let agg_time = aggregates.entry(time.time().clone()).or_insert_with(HashMap::new);
                        for (key, val) in vector.drain(..) {
                            let agg = agg_time.entry(key.clone()).or_insert_with(Default::default);
                            fold(&key, val, agg);
                        }
                        notificator.notify_at(time.retain());
                    });

                    input2.for_each(|time,_data| { spec_notif.notify_at(time.retain()); });

                    spec_notif.for_each(|time,_,_| {
                        if let Some(aggs) = aggregates.get(time.time()) {
                            let mut session = output.session(&time);
                            for (key, agg) in aggs {
                                session.give(emit((*key).clone(), (*agg).clone()));
                            }
                        }
                    });

                    // pop completed aggregates, send along whatever
                    notificator.for_each(|time,_,_| {
                        if let Some(aggs) = aggregates.remove(time.time()) {
                            let mut session = output.session(&time);
                            for (key, agg) in aggs {
                                session.give(emit(key, agg));
                            }
                        }
                    });
                }
            }
        )
    }
}
