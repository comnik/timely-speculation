# Timely Speculation

Experimenting with speculative results in timely.

The idea here was to take timely's `Aggregate` operator and give it a
second input, that is only used to propagate a speculative
frontier. The implementation is a complete hack but revolves around a
separate notificator giving results without affecting operator state:

``` rust
spec_notif.for_each(|time,_,_| {
    if let Some(aggs) = aggregates.get(time.time()) {
        let mut session = output.session(&time);
        for (key, agg) in aggs {
            session.give(emit((*key).clone(), (*agg).clone()));
        }
    }
});
```

Once the actual input advances, the actual notificator triggers the
correct outputs and cleans up as usual.

``` rust
// pop completed aggregates, send along whatever
notificator.for_each(|time,_,_| {
    if let Some(aggs) = aggregates.remove(time.time()) {
        let mut session = output.session(&time);
        for (key, agg) in aggs {
            session.give(emit(key, agg));
        }
    }
});
```

Without speculative results, we can close windows as such:

``` rust
cap = cap.delayed(&RootTimestamp::new(window));
```

Doing so means that we'll have to discard late arrivals. Ofcourse
eventually we will have to do this, to avoid operator state growing
unbounded. A speculative frontier allows us to advance the (otherwise
non-contributing) speculative input and get results, before
downgrading our capability to input late data.

The demo implemented in [src/main.rs](src/main.rs) is based on the
excellent ["Dataflow: A Unified Model for Batch and Streaming Data
Processing"](https://www.youtube.com/watch?v=3UfZN59Nsk8) talk by
Frances Perry.
