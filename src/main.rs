use csv::ReaderBuilder;
use csv::WriterBuilder;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use google_maps::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::default::Default;
use std::env;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct Id {
    videoId: String,
}

#[derive(Deserialize, Debug)]
struct Item {
    id: Id,
}

#[derive(Deserialize, Debug)]
struct SearchResult {
    items: Vec<Item>,
}

async fn search(query: &str, key: &str) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let mut xs = HashSet::new();
    let url = env::var("QUERY_URL_BASE")? + "&key=" + key + "&q=" + query;
    let body = reqwest::get(url).await?.json::<SearchResult>().await?;
    for item in body.items {
        xs.insert(item.id.videoId);
    }
    println!("search succeeded");
    Ok(xs)
}

async fn get_queries() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let url = env::var("QUERIES_URL")?;
    let gzip = reqwest::get(url).await?.bytes().await?;
    let mut d = GzDecoder::new(&*gzip);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();
    let v: Vec<&str> = s.split('\n').collect();
    let mut ret: Vec<String> = vec![];
    for id in v {
        ret.push(id.to_string());
    }
    Ok(ret)
}

async fn get_id_list() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let xs = get_queries().await?;
    let mut ids = HashSet::<String>::new();
    let keys = vec![
        env::var("DEVELOPER_KEY0")?,
        env::var("DEVELOPER_KEY1")?,
        env::var("DEVELOPER_KEY2")?,
    ];
    let mut i = 0;
    for query in xs {
        loop {
            match search(&query, &keys[i]).await {
                Ok(ret) => {
                    ids.extend(ret);
                    break;
                }
                Err(err) => {
                    eprintln!("{}", err);
                    i += 1;
                    if i == keys.len() {
                        return Ok(ids);
                    }
                    eprintln!("try next key");
                }
            }
        }
    }
    Ok(ids)
}

async fn get_previous_id_list() -> Result<HashMap<String, (f64, f64)>, Box<dyn std::error::Error>> {
    let mut hm = HashMap::<String, (f64, f64)>::new();
    let url = env::var("DATA_URL")?;
    let gzip = reqwest::get(url).await?.bytes().await?;
    let mut d = GzDecoder::new(&*gzip);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();
    let mut rdr = ReaderBuilder::new().from_reader(s.as_bytes());
    while let Some(result) = rdr.records().next() {
        let record = result?;
        let id = &record[2];
        let lat = record[0].parse()?;
        let lng = record[1].parse()?;
        hm.insert(id.to_string(), (lat, lng));
    }
    Ok(hm)
}

#[derive(Deserialize, Debug)]
struct Location {
    latitude: f64,
    longitude: f64,
}

#[derive(Deserialize, Debug)]
struct RecordingDetails {
    location: Location,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct VideoItem {
    recordingDetails: RecordingDetails,
}

#[derive(Deserialize, Debug)]
struct VideoResult {
    items: Vec<VideoItem>,
}

#[derive(Debug)]
struct MyError(String);

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}

impl std::error::Error for MyError {}

async fn get_location(id: &str, key: &str) -> Result<(f64, f64), Box<dyn std::error::Error>> {
    let url = env::var("LOCATION_URL_BASE")? + "&key=" + key + "&id=" + id;
    let body = reqwest::get(url).await?.json::<VideoResult>().await?;
    if !body.items.is_empty() {
        let location = &body.items[0].recordingDetails.location;
        let ret = (location.latitude, location.longitude);
        if ret == (0.0, 0.0) {
            Err(Box::new(MyError("location not found".into())))
        } else {
            Ok(ret)
        }
    } else {
        Err(Box::new(MyError("location not found".into())))
    }
}

#[derive(Deserialize, Debug)]
struct VideoInfo {
    title: String,
    author_name: String,
}

async fn get_info(id: &str) -> Result<[String; 2], Box<dyn std::error::Error>> {
    let url = env::var("INFO_URL_BASE")? + "?v=" + id + "&format=json";
    let body = reqwest::get(url).await?.json::<VideoInfo>().await?;
    Ok([body.title, body.author_name])
}

async fn get_location2(
    id: &str,
    client: &ClientSettings,
) -> Result<(f64, f64), Box<dyn std::error::Error>> {
    let info = get_info(id).await?;
    for address in info {
        let location = client.geocoding().with_address(&address).execute().await?;
        match location.results.first() {
            Some(result) => {
                return Ok((
                    result.geometry.location.lat.to_f64().unwrap(),
                    result.geometry.location.lng.to_f64().unwrap(),
                ));
            }
            None => continue,
        }
    }
    Err(Box::new(MyError("location not found".into())))
}

#[derive(Debug, Serialize)]
struct Record<'a> {
    lat: f64,
    lng: f64,
    id: &'a str,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct Snippet {
    liveBroadcastContent: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct VideoItem2 {
    snippet: Snippet,
}

#[derive(Deserialize, Debug)]
struct VideoResult2 {
    items: Vec<VideoItem2>,
}

async fn is_live(id: &str, key: &str) -> bool {
    match env::var("LIVE_URL_BASE") {
        Ok(url_base) => {
            let url = url_base + "&key=" + key + "&id=" + id;
            match reqwest::get(url).await {
                Ok(response) => match response.json::<VideoResult2>().await {
                    Ok(body) => {
                        if !body.items.is_empty() {
                            &body.items[0].snippet.liveBroadcastContent == "live"
                        } else {
                            false
                        }
                    }
                    Err(_) => false,
                },
                Err(_) => false,
            }
        }
        Err(_) => false,
    }
}

async fn remove_garbage(key: &str, locations: &mut HashMap<String, (f64, f64)>) {
    let mut v: Vec<String> = vec![];
    for (id, _) in locations.iter() {
        if !is_live(id, key).await {
            v.push(id.to_string());
        }
    }
    for id in v {
        locations.remove(&id);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut locations = get_previous_id_list().await?;
    let key = env::var("DEVELOPER_KEY2")?;
    remove_garbage(&key, &mut locations).await;
    let ids = get_id_list().await?;
    let mut undefined = HashSet::<&str>::new();
    for id in &ids {
        match get_location(id, &key).await {
            Ok(location) => {
                println!("location found");
                locations.insert(id.to_string(), location);
            }
            Err(_) => {
                println!("location not found");
                undefined.insert(id);
            }
        }
    }
    let google_maps_client = ClientSettings::new(&env::var("GOOGLE_API_KEY")?);
    for id in undefined {
        match get_location2(id, &google_maps_client).await {
            Ok(location) => {
                println!("location2 found");
                locations.insert(id.to_string(), location);
            }
            Err(_) => {
                println!("location2 not found");
            }
        }
    }
    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(vec![]);
    for (k, v) in locations {
        wtr.serialize(Record {
            lat: v.0,
            lng: v.1,
            id: &k,
        })?;
    }
    let data = wtr.into_inner()?;
    let file = File::create("geo.csv.gz")?;
    GzEncoder::new(file, Compression::default()).write_all(&data)?;
    Ok(())
}
