use pgx::*;
use pgx::prelude::*;

#[pg_extern]
fn asof(t1:String,
        t2:String,
        time_column:String,
        value_column:String) -> TableIterator<'static, (name!(time, Option<TimestampWithTimeZone>), name!(value, Option<f64>))> {

    let mut table_one_query:String = "select ".to_owned();
    table_one_query.push_str(&time_column);
    table_one_query.push_str(",null as ");
    table_one_query.push_str(&value_column);
    table_one_query.push_str(" from ");
    table_one_query.push_str(&t1);

    // let table_two_query = "select time,val from sample_data_second";
    let mut table_two_query:String ="select ".to_owned();
    table_two_query.push_str(&time_column);
    table_two_query.push_str(",");
    table_two_query.push_str(&value_column);
    table_two_query.push_str(" from  ");
    table_two_query.push_str(&t2);
    let table_two_query = &table_two_query;
    let table_one_query = &table_one_query;

    let mut results = Vec::new();
    Spi::connect(|client| {
        client
            .select(table_one_query, None, None)
            .map(|row| (row[1].value(), row[2].value()))
            .for_each(|tuple| results.push(tuple));
        client
            .select(table_two_query, None, None)
            .map(|row| (row[1].value(), row[2].value()))
            .for_each(|tuple| results.push(tuple));
        Ok(Some(()))
    });
   results.sort_by(|a, b| b.0.cmp(&a.0));
    let mut results2 = Vec::new();
    let mut curr_val = None;

    for mut res in results{
        if res.1 == None{
            if curr_val == None{}else{
                res.1 = curr_val;
            }
        }else{
            curr_val = res.1;
        }
        results2.push((res.0,res.1));
    }

    TableIterator::new(results2.into_iter())
}