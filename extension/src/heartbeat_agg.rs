use pgx::iter::TableIterator;
use pgx::*;

use crate::{
    aggregate_utils::in_aggregate_context,
    datum_utils::interval_to_ms,
    flatten,
    palloc::{Inner, InternalAsValue, ToInternal},
    pg_type,
    raw::{Interval, TimestampTz},
    ron_inout_funcs,
};

use toolkit_experimental::HeartbeatAggData;

const BUFFER_SIZE: usize = 1000; // How many values to absorb before consolidating

// Given the lack of a good range map class, or efficient predecessor operation on btrees,
// the trans state will simply collect points and then process them in batches
pub struct HeartbeatTransState {
    start: i64,
    end: i64,
    interval_len: i64,
    buffer: Vec<i64>,
    liveness: Vec<(i64, i64)>, // sorted array of non-overlapping (start_time, end_time)
}

impl HeartbeatTransState {
    pub fn new(start: i64, end: i64, interval: i64) -> Self {
        HeartbeatTransState {
            start,
            end,
            interval_len: interval,
            buffer: vec![],
            liveness: vec![],
        }
    }

    pub fn insert(&mut self, time: i64) {
        assert!(time >= self.start && time < self.end);
        if self.buffer.len() >= BUFFER_SIZE {
            self.process_batch();
        }
        self.buffer.push(time);
    }

    pub fn process_batch(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_unstable();

        let mut new_intervals = vec![];

        let mut start = *self.buffer.first().unwrap();
        let mut bound = start + self.interval_len;

        for heartbeat in std::mem::take(&mut self.buffer).into_iter() {
            if heartbeat <= bound {
                bound = heartbeat + self.interval_len;
            } else {
                new_intervals.push((start, bound));
                start = heartbeat;
                bound = start + self.interval_len;
            }
        }
        new_intervals.push((start, bound));

        if self.liveness.is_empty() {
            std::mem::swap(&mut self.liveness, &mut new_intervals);
        } else {
            self.combine_intervals(new_intervals)
        }
    }

    fn combine_intervals(&mut self, new_intervals: Vec<(i64, i64)>) {
        // Optimized path for ordered inputs
        if self.liveness.last().unwrap().0 < new_intervals.first().unwrap().0 {
            let mut new_intervals = new_intervals.into_iter();

            // Grab the first new interval to check for overlap with the existing data
            let first_new = new_intervals.next().unwrap();

            if self.liveness.last().unwrap().1 >= first_new.0 {
                // Note that the bound of the new interval must be >= the old bound
                self.liveness.last_mut().unwrap().1 = first_new.1;
            } else {
                self.liveness.push(first_new);
            }

            for val in new_intervals {
                self.liveness.push(val);
            }
            return;
        }

        let new_intervals = new_intervals.into_iter();
        let old_intervals = std::mem::take(&mut self.liveness).into_iter();

        // In the following while let block, test and control are used to track our two interval iterators.
        // We will swap them back and forth to try to keep control as the iterator which has provided the current bound.
        let mut test = new_intervals.peekable();
        let mut control = old_intervals.peekable();

        while let Some(interval) = if let Some((start1, _)) = control.peek() {
            if let Some((start2, _)) = test.peek() {
                let (start, mut bound) = if start1 < start2 {
                    control.next().unwrap()
                } else {
                    std::mem::swap(&mut test, &mut control);
                    control.next().unwrap()
                };

                while test.peek().is_some() && test.peek().unwrap().0 <= bound {
                    let (_, new_bound) = test.next().unwrap();
                    if new_bound > bound {
                        std::mem::swap(&mut test, &mut control);
                        bound = new_bound;
                    }
                }

                Some((start, bound))
            } else {
                control.next()
            }
        } else {
            test.next()
        } {
            self.liveness.push(interval)
        }
    }

    pub fn combine(&mut self, mut other: HeartbeatTransState) {
        assert!(self.interval_len == other.interval_len); // Nicer error would be nice here
        self.process_batch();
        other.process_batch();
        self.combine_intervals(other.liveness);
    }
}

#[cfg(any(test, feature = "pg_test"))]
impl HeartbeatTransState {
    pub fn get_buffer(&self) -> &Vec<i64> {
        &self.buffer
    }
    pub fn get_liveness(&self) -> &Vec<(i64, i64)> {
        &self.liveness
    }
}

#[pg_schema]
mod toolkit_experimental {
    use super::*;

    pg_type! {
        #[derive(Debug)]
        struct HeartbeatAgg<'input>
        {
            start_time : i64,
            end_time : i64,
            interval_len : i64,
            num_intervals : u64,
            interval_starts : [i64; self.num_intervals],
            interval_ends : [i64; self.num_intervals],
        }
    }

    ron_inout_funcs!(HeartbeatAgg);

    impl HeartbeatAgg<'_> {
        fn sum_live_intervals(self) -> i64 {
            let starts = self.interval_starts.as_slice();
            let ends = self.interval_ends.as_slice();
            let mut sum = 0;
            for i in 0..self.num_intervals as usize {
                sum += ends[i] - starts[i];
            }
            sum
        }
    }

    #[pg_extern]
    pub fn live_ranges(
        agg: HeartbeatAgg<'static>,
    ) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
        let starts = agg.interval_starts.clone();
        let ends = agg.interval_ends.clone();
        TableIterator::new(
            starts
                .into_iter()
                .map(|x| x.into())
                .zip(ends.into_iter().map(|x| x.into())),
        )
    }

    #[pg_extern]
    pub fn dead_ranges(
        agg: HeartbeatAgg<'static>,
    ) -> TableIterator<'static, (name!(start, TimestampTz), name!(end, TimestampTz))> {
        if agg.num_intervals == 0 {
            return TableIterator::new(std::iter::once((
                agg.start_time.into(),
                agg.end_time.into(),
            )));
        }

        // Dead ranges are the opposite of the intervals stored in the aggregate
        let mut starts = agg.interval_ends.clone().into_vec();
        let mut ends = agg.interval_starts.clone().into_vec();

        // Fix the first point depending on whether the aggregate starts in a live or dead range
        if ends[0] == agg.start_time {
            ends.remove(0);
        } else {
            starts.insert(0, agg.start_time);
        }

        // Fix the last point depending on whether the aggregate starts in a live or dead range
        if *starts.last().unwrap() == agg.end_time {
            starts.pop();
        } else {
            ends.push(agg.end_time);
        }

        TableIterator::new(
            starts
                .into_iter()
                .map(|x| x.into())
                .zip(ends.into_iter().map(|x| x.into())),
        )
    }

    #[pg_extern]
    pub fn duration_live(agg: HeartbeatAgg<'static>) -> Interval {
        agg.sum_live_intervals().into()
    }

    #[pg_extern]
    pub fn duration_dead(agg: HeartbeatAgg<'static>) -> Interval {
        (agg.end_time - agg.start_time - agg.sum_live_intervals()).into()
    }

    #[pg_extern]
    pub fn live_at(agg: HeartbeatAgg<'static>, test: TimestampTz) -> bool {
        if agg.num_intervals == 0 {
            return false;
        }

        let test = i64::from(test);
        let mut start_iter = agg.interval_starts.iter().enumerate().peekable();
        while let Some((idx, val)) = start_iter.next() {
            if test < val {
                // Only possible if test shows up before first interval
                return false;
            }
            if let Some((_, next_val)) = start_iter.peek() {
                if test < *next_val {
                    return test < agg.interval_ends.as_slice()[idx];
                }
            }
        }
        // Fall out the loop if test > start of last interval
        return test < *agg.interval_ends.as_slice().last().unwrap();
    }
}

impl From<toolkit_experimental::HeartbeatAgg<'static>> for HeartbeatTransState {
    fn from(agg: toolkit_experimental::HeartbeatAgg<'static>) -> Self {
        HeartbeatTransState {
            start: agg.start_time,
            end: agg.end_time,
            interval_len: agg.interval_len,
            buffer: vec![],
            liveness: agg
                .interval_starts
                .iter()
                .zip(agg.interval_ends.iter())
                .collect(),
        }
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn heartbeat_trans(
    state: Internal,
    heartbeat: TimestampTz,
    start: TimestampTz,
    length: Interval,
    liveness_duration: Interval,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    heartbeat_trans_inner(
        unsafe { state.to_inner() },
        heartbeat,
        start,
        length,
        liveness_duration,
        fcinfo,
    )
    .internal()
}
pub fn heartbeat_trans_inner(
    state: Option<Inner<HeartbeatTransState>>,
    heartbeat: TimestampTz,
    start: TimestampTz,
    length: Interval,
    liveness_duration: Interval,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<HeartbeatTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = state.unwrap_or_else(|| {
                let length = interval_to_ms(&start, &length);
                let interval = interval_to_ms(&start, &liveness_duration);
                let start = start.into();
                HeartbeatTransState::new(start, start + length, interval).into()
            });
            state.insert(heartbeat.into());
            Some(state)
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn heartbeat_final(
    state: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::HeartbeatAgg<'static>> {
    heartbeat_final_inner(unsafe { state.to_inner() }, fcinfo)
}
pub fn heartbeat_final_inner(
    state: Option<Inner<HeartbeatTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<toolkit_experimental::HeartbeatAgg<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            state.map(|mut s| {
                s.process_batch();
                let (starts, mut ends): (Vec<i64>, Vec<i64>) =
                    s.liveness.clone().into_iter().unzip();

                // Trim last interval to end of aggregate's range
                if let Some(last) = ends.last_mut() {
                    if *last > s.end {
                        *last = s.end;
                    }
                }

                flatten!(HeartbeatAgg {
                    start_time: s.start,
                    end_time: s.end,
                    interval_len: s.interval_len,
                    num_intervals: starts.len() as u64,
                    interval_starts: starts.into(),
                    interval_ends: ends.into(),
                })
            })
        })
    }
}

#[pg_extern(schema = "toolkit_experimental", immutable, parallel_safe)]
pub fn heartbeat_rollup_trans(
    state: Internal,
    value: Option<toolkit_experimental::HeartbeatAgg<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    heartbeat_rollup_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn heartbeat_rollup_trans_inner(
    state: Option<Inner<HeartbeatTransState>>,
    value: Option<toolkit_experimental::HeartbeatAgg<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<HeartbeatTransState>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state, value) {
            (a, None) => a,
            (None, Some(a)) => Some(HeartbeatTransState::from(a).into()),
            (Some(mut a), Some(b)) => {
                a.combine(b.into());
                Some(a)
            }
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.heartbeat_agg(\n\
        heartbeat TIMESTAMPTZ, agg_start TIMESTAMPTZ, agg_duration INTERVAL, heartbeat_liveness INTERVAL\n\
    ) (\n\
        sfunc = toolkit_experimental.heartbeat_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.heartbeat_final\n\
    );\n\
",
    name = "heartbeat_agg",
    requires = [
        heartbeat_trans,
        heartbeat_final,
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE toolkit_experimental.rollup(\n\
        toolkit_experimental.HeartbeatAgg\n\
    ) (\n\
        sfunc = toolkit_experimental.heartbeat_rollup_trans,\n\
        stype = internal,\n\
        finalfunc = toolkit_experimental.heartbeat_final\n\
    );\n\
",
    name = "heartbeat_agg_rollup",
    requires = [heartbeat_rollup_trans, heartbeat_final,],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    pub fn test_heartbeat_trans_state() {
        let mut state = HeartbeatTransState::new(0, 500, 10);
        state.insert(100);
        state.insert(200);
        state.insert(250);
        state.insert(220);
        state.insert(210);
        state.insert(300);

        assert_eq!(state.get_buffer().len(), 6);

        state.process_batch();
        assert_eq!(state.get_buffer().len(), 0);

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (200, 230));
        assert_eq!(*it.next().unwrap(), (250, 260));
        assert_eq!(*it.next().unwrap(), (300, 310));
        assert!(it.next().is_none());

        state.insert(400);
        state.insert(350);
        state.process_batch();

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (200, 230));
        assert_eq!(*it.next().unwrap(), (250, 260));
        assert_eq!(*it.next().unwrap(), (300, 310));
        assert_eq!(*it.next().unwrap(), (350, 360));
        assert_eq!(*it.next().unwrap(), (400, 410));
        assert!(it.next().is_none());

        state.insert(80);
        state.insert(190);
        state.insert(210);
        state.insert(230);
        state.insert(240);
        state.insert(310);
        state.insert(395);
        state.insert(408);
        state.process_batch();

        let mut it = state.get_liveness().iter();
        assert_eq!(*it.next().unwrap(), (80, 90));
        assert_eq!(*it.next().unwrap(), (100, 110));
        assert_eq!(*it.next().unwrap(), (190, 260));
        assert_eq!(*it.next().unwrap(), (300, 320));
        assert_eq!(*it.next().unwrap(), (350, 360));
        assert_eq!(*it.next().unwrap(), (395, 418));
        assert!(it.next().is_none());
    }

    #[pg_test]
    pub fn test_heartbeat_agg() {
        Spi::execute(|client| {
            client.select("SET TIMEZONE to UTC", None, None);

            client.select("CREATE TABLE liveness(heartbeat TIMESTAMPTZ)", None, None);

            client.select(
                "INSERT INTO liveness VALUES
                ('01-01-2020 0:2:20 UTC'),
                ('01-01-2020 0:10 UTC'),
                ('01-01-2020 0:17 UTC'),
                ('01-01-2020 0:30 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:35 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:40 UTC'),
                ('01-01-2020 0:50:30 UTC'),
                ('01-01-2020 1:00 UTC'),
                ('01-01-2020 1:08 UTC'),
                ('01-01-2020 1:18 UTC'),
                ('01-01-2020 1:28 UTC'),
                ('01-01-2020 1:38:01 UTC'),
                ('01-01-2020 1:40 UTC'),
                ('01-01-2020 1:40:01 UTC'),
                ('01-01-2020 1:50:01 UTC'),
                ('01-01-2020 1:57 UTC'),
                ('01-01-2020 1:59:50 UTC')
            ",
                None,
                None,
            );

            let mut result = client.select(
                "SELECT toolkit_experimental.live_ranges(toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, None);

            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:02:20+00\",\"2020-01-01 00:27:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:30:00+00\",\"2020-01-01 00:50:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:50:30+00\",\"2020-01-01 01:38:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 01:38:01+00\",\"2020-01-01 02:00:00+00\")"
            );
            assert!(result.next().is_none());

            let mut result = client.select(
                "SELECT toolkit_experimental.dead_ranges(toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, None);

            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:00:00+00\",\"2020-01-01 00:02:20+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:27:00+00\",\"2020-01-01 00:30:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:50:00+00\",\"2020-01-01 00:50:30+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 01:38:00+00\",\"2020-01-01 01:38:01+00\")"
            );
            assert!(result.next().is_none());

            let result = client.select(
                "SELECT toolkit_experimental.duration_live(toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, None).first().get_one::<String>().unwrap();
            assert_eq!("01:54:09", result);

            let result = client.select(
                "SELECT toolkit_experimental.duration_dead(toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m'))::TEXT
                FROM liveness", None, None).first().get_one::<String>().unwrap();
            assert_eq!("00:05:51", result);

            let (result1, result2, result3) =
                client.select(
                    "WITH agg AS (SELECT toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT toolkit_experimental.live_at(agg, '01-01-2020 00:01:00 UTC')::TEXT, 
                    toolkit_experimental.live_at(agg, '01-01-2020 00:05:00 UTC')::TEXT,
                    toolkit_experimental.live_at(agg, '01-01-2020 00:30:00 UTC')::TEXT FROM agg", None, None)
                .first()
                .get_three::<String, String, String>();

            let (result4, result5) =
                client.select(
                    "WITH agg AS (SELECT toolkit_experimental.heartbeat_agg(heartbeat, '01-01-2020 UTC', '2h', '10m') AS agg FROM liveness)
                    SELECT toolkit_experimental.live_at(agg, '01-01-2020 01:38:00 UTC')::TEXT,
                    toolkit_experimental.live_at(agg, '01-01-2020 02:01:00 UTC')::TEXT FROM agg", None, None)
                .first()
                .get_two::<String, String>();

            assert_eq!(result1.unwrap(), "false"); // outside ranges
            assert_eq!(result2.unwrap(), "true"); // inside ranges
            assert_eq!(result3.unwrap(), "true"); // first point of range
            assert_eq!(result4.unwrap(), "false"); // last point of range
            assert_eq!(result5.unwrap(), "false"); // outside aggregate
        })
    }

    #[pg_test]
    pub fn test_heartbeat_rollup() {
        Spi::execute(|client| {
            client.select("SET TIMEZONE to UTC", None, None);

            client.select(
                "CREATE TABLE aggs(agg toolkit_experimental.heartbeatagg)",
                None,
                None,
            );

            client.select(
                "INSERT INTO aggs SELECT toolkit_experimental.heartbeat_agg(hb, '01-01-2020 UTC', '1h', '10m')
                FROM (VALUES
                    ('01-01-2020 0:2:20 UTC'::timestamptz),
                    ('01-01-2020 0:10 UTC'::timestamptz),
                    ('01-01-2020 0:17 UTC'::timestamptz),
                    ('01-01-2020 0:30 UTC'::timestamptz),
                    ('01-01-2020 0:35 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 0:50:30 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    None,
                );

            client.select(
                "INSERT INTO aggs SELECT toolkit_experimental.heartbeat_agg(hb, '01-01-2020 0:30 UTC', '1h', '10m')
                    FROM (VALUES
                    ('01-01-2020 0:35 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 0:40 UTC'::timestamptz),
                    ('01-01-2020 1:08 UTC'::timestamptz),
                    ('01-01-2020 1:18 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    None,
                );

            client.select(
                "INSERT INTO aggs SELECT toolkit_experimental.heartbeat_agg(hb, '01-01-2020 1:00 UTC', '1h', '10m')
                FROM (VALUES
                    ('01-01-2020 1:00 UTC'::timestamptz),
                    ('01-01-2020 1:28 UTC'::timestamptz),
                    ('01-01-2020 1:38:01 UTC'::timestamptz),
                    ('01-01-2020 1:40 UTC'::timestamptz),
                    ('01-01-2020 1:40:01 UTC'::timestamptz),
                    ('01-01-2020 1:50:01 UTC'::timestamptz),
                    ('01-01-2020 1:57 UTC'::timestamptz),
                    ('01-01-2020 1:59:50 UTC'::timestamptz)
                ) AS _(hb)",
                    None,
                    None,
                );

            let mut result = client.select(
                "SELECT toolkit_experimental.dead_ranges(toolkit_experimental.rollup(agg))::TEXT
                FROM aggs",
                None,
                None,
            );

            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:00:00+00\",\"2020-01-01 00:02:20+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:27:00+00\",\"2020-01-01 00:30:00+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 00:50:00+00\",\"2020-01-01 00:50:30+00\")"
            );
            assert_eq!(
                result.next().unwrap()[1].value::<String>().unwrap(),
                "(\"2020-01-01 01:38:00+00\",\"2020-01-01 01:38:01+00\")"
            );
            assert!(result.next().is_none());
        });
    }
}
