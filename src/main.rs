use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
    locations: HashMap<String, Location>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
struct Location {
    longitude: f32,
    latitude: f32,
}

#[derive(Debug)]
struct Row {
    data: String,
    timestamp: String,
}

fn may_show(point: &Location, points: &[Location]) -> bool {
    const NEIGHBORS: u8 = 3;
    const DISTANCE: f32 = 100.0;
    let mut found = 0;
    for candidate in points {
        if (get_distance(candidate, point)) < DISTANCE {
            found += 1
        }
        if found == NEIGHBORS {
            return true;
        }
    }
    false
}

fn get_distance(point: &Location, other_point: &Location) -> f32 {
    const R: f32 = 6_371_000f32;

    let lon1 = point.longitude.to_radians();
    let lat1 = point.latitude.to_radians();

    let lon2 = other_point.longitude.to_radians();
    let lat2 = other_point.latitude.to_radians();

    let dlon = lon2 - lon1;
    let dlat = lat2 - lat1;

    let a: f32 =
        (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * ((dlon / 2.0).sin()).powi(2);
    let c: f32 = 2.0 * (a.sqrt()).atan2((1f32 - a).sqrt());

    R * c
}

#[derive(Debug, Deserialize, Serialize)]
struct ResultRow {
    data: Vec<Feature>,
    timestamp: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Point {
    r#type: String, // Always "Point
    coordinates: [f32; 2],
}

#[derive(Debug, Deserialize, Serialize)]
struct Feature {
    r#type: String, // Always "Feature"
    geometry: Point,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let filepath = &args[1];
    let start = &args[2];
    let end = &args[3];
    let db = Connection::open(filepath)?;
    eprintln!("{} {}", start, end);

    let mut stmt =
        db.prepare("select timestamp, data from tracks where timestamp >= (?1) and timestamp <= (?2)")?;
    let result_iter = stmt.query_map([start, end], |row| {
        Ok(Row {
            timestamp: row.get(0)?,
            data: row.get(1)?,
        })
    })?;

    let mut results: Vec<ResultRow> = Vec::new();
    for row in result_iter {
        let res = row.unwrap();
        let entry: Entry = serde_json::from_str(res.data.as_str()).unwrap();
        let points = entry
            .locations
            .values()
            .into_iter()
            .map(|l| Location {
                latitude: l.latitude as f32 / 1_000_000.,
                longitude: l.longitude as f32 / 1_000_000.,
            })
            .collect::<Vec<Location>>();
        let mut filtered_points: Vec<Feature> = Vec::new();
        for point in &points {
            if may_show(point, &points) {
                filtered_points.push(Feature {
                    r#type: "Feature".into(),
                    geometry: Point {
                        r#type: "Point".into(),
                        coordinates: [point.longitude, point.latitude],
                    },
                })
            }
        }
        eprintln!("{}", res.timestamp);
        if !filtered_points.is_empty() {
            results.push(ResultRow {
                timestamp: res.timestamp,
                data: filtered_points,
            })
        }
    }
    let json = serde_json::to_string(&results).unwrap();
    println!("{}", json);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{get_distance, Location};

    #[test]
    fn distance_between_berlin_and_paris() {
        let berlin = Location {
            longitude: 13.405,
            latitude: 52.52,
        };
        let paris = Location {
            longitude: 2.352222,
            latitude: 48.856613,
        };
        let result = get_distance(&berlin, &paris);
        let expected = 878_000.0;
        let delta = (expected - result).abs();
        // We are on a national level, allow a delta of up to 5km
        assert!(delta < 5_000f32);
    }

    #[test]
    fn distance_between_kegel_and_ostbloc() {
        let kegel = Location {
            longitude: 13.45490,
            latitude: 52.50722,
        };
        let ostbloc = Location {
            longitude: 13.48866,
            latitude: 52.49137,
        };
        let result = get_distance(&kegel, &ostbloc);
        let expected = 2.87 * 1000.0;
        let delta = (expected - result).abs();
        // We are on a city level, only allow 20 meters delta
        assert!(delta < 20f32);
    }
}
