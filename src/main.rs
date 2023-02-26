use csv::ReaderBuilder;
use csv::WriterBuilder;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use google_maps::prelude::*;
use regex::Regex;
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

async fn get_blacklist() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let url = env::var("BLACKLIST_URL")?;
    let gzip = reqwest::get(url).await?.bytes().await?;
    let mut d = GzDecoder::new(&*gzip);
    let mut s = String::new();
    d.read_to_string(&mut s).unwrap();
    let v: Vec<&str> = s.split('\n').collect();
    let mut ret = HashSet::<String>::new();
    for id in v {
        ret.insert(id.to_string());
    }
    Ok(ret)
}

async fn get_id_list() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let xs = get_queries().await?;
    let mut ids = get_watchs().await?;
    let keys = vec![
        env::var("DEVELOPER_KEY0")?,
        env::var("DEVELOPER_KEY1")?,
        env::var("DEVELOPER_KEY2")?,
        env::var("DEVELOPER_KEY3")?,
        env::var("DEVELOPER_KEY4")?,
    ];
    let mut i = 0;
    let total = xs.len();
    for (count, query) in xs.into_iter().enumerate() {
        println!("search {count}/{total}");
        loop {
            match search(&query, &keys[i]).await {
                Ok(ret) => {
                    ids.extend(ret);
                    break;
                }
                Err(err) => {
                    eprintln!("{err}");
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

async fn get_location(id: &str) -> Result<(f64, f64), Box<dyn std::error::Error>> {
    let key = env::var("DEVELOPER_KEY4")?;
    let url = env::var("LOCATION_URL_BASE")? + "&key=" + &key + "&id=" + id;
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

async fn get_location2(id: &str, client: &ClientSettings) -> Result<(f64, f64), String> {
    match get_info(id).await {
        Ok(info) => {
            let address = info.join(" ");
            match client.geocoding().with_address(&address).execute().await {
                Ok(location) => match location.results.first() {
                    Some(result) => Ok((
                        result.geometry.location.lat.to_f64().unwrap(),
                        result.geometry.location.lng.to_f64().unwrap(),
                    )),
                    None => Err(address),
                },
                Err(err) => {
                    eprintln!("{err}");
                    Err(address)
                }
            }
        }
        Err(err) => {
            eprintln!("{err}");
            Err("".to_string())
        }
    }
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

async fn is_live(id: &str, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let url = env::var("LIVE_URL_BASE")? + "&key=" + key + "&id=" + id;
    let response = reqwest::get(url).await?;
    if response.status().as_u16() == 403 {
        println!("exceeded youtube quota");
        return Ok(false);
    }
    let body = response.json::<VideoResult2>().await?;
    if !body.items.is_empty() {
        if &body.items[0].snippet.liveBroadcastContent == "live" {
            Ok(true)
        } else {
            Err("not live".into())
        }
    } else {
        Err("body is empty".into())
    }
}

async fn remove_garbage(
    locations: &mut HashMap<String, (f64, f64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let keys = vec![
        env::var("DEVELOPER_KEY4")?,
        env::var("DEVELOPER_KEY3")?,
        env::var("DEVELOPER_KEY2")?,
        env::var("DEVELOPER_KEY1")?,
        env::var("DEVELOPER_KEY0")?,
    ];
    let mut i = 0;
    let mut v: Vec<String> = vec![];
    for (count, (id, _)) in locations.iter().enumerate() {
        let locations_len = locations.len();
        println!("checking {count}/{locations_len}");
        loop {
            match is_live(id, &keys[i]).await {
                Ok(p) => {
                    if p {
                        break;
                    } else {
                        i += 1;
                        if i >= keys.len() {
                            return Err("ran out of keys".into());
                        }
                    }
                }
                Err(err) => {
                    println!("invalid: {err}");
                    v.push(id.to_string());
                    break;
                }
            }
        }
    }
    for id in v {
        locations.remove(&id);
    }
    Ok(())
}

async fn write_geo(
    locations: HashMap<String, (f64, f64)>,
) -> Result<(), Box<dyn std::error::Error>> {
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

async fn write_hash_set(
    blacklist: HashSet<String>,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(filename)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    for id in blacklist {
        match encoder.write_fmt(format_args!("{id}\n")) {
            Ok(_) => {}
            Err(_) => {
                println!("location not found");
                break;
            }
        }
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct Request {
    totalResults: String,
    count: i32,
    startIndex: i32,
}

#[derive(Deserialize, Debug)]
struct Queries {
    request: Vec<Request>,
}

#[derive(Deserialize, Debug)]
struct SnippetItem {
    snippet: String,
}

#[derive(Deserialize, Debug)]
struct Watches {
    queries: Queries,
    items: Vec<SnippetItem>,
}

async fn get_watchs() -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let mut set = HashSet::<String>::new();
    let mut start = 1;
    loop {
        println!("get_watchs: start = {start}");
        let url = env::var("WATCH_URL")?.to_owned() + &start.to_string();
        let body = reqwest::get(url).await?.json::<Watches>().await?;
        let re = Regex::new(r"www\.youtube\.com/watch\?v=(.{11})").unwrap();
        for item in body.items {
            if let Some(caps) = re.captures(&item.snippet) {
                if let Some(s) = caps.get(1) {
                    set.insert(s.as_str().to_string());
                }
            }
        }
        let request = &body.queries.request[0];
        let total: i32 = request.totalResults.parse().unwrap();
        let next = request.startIndex + request.count;
        if next > total {
            break;
        }
        if next > 100 {
            break;
        } else {
            start = next;
        }
    }
    Ok(set)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args();
    let mut locations = get_previous_id_list().await?;
    let current_count = locations.len();
    remove_garbage(&mut locations).await?;
    if args.len() == 1 {
        let ids = get_id_list().await?;
        let total = ids.len();
        let mut blacklist = get_blacklist().await?;
        let mut undefined = HashSet::<&str>::new();
        let mut non_live_camera = HashSet::<String>::new();
        for (count, id) in ids.iter().enumerate() {
            println!("location {count}/{total}");
            if blacklist.contains(id) {
                continue;
            }
            if locations.contains_key(id) {
                continue;
            }
            match get_location(id).await {
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
        let total = undefined.len();
        for (count, id) in undefined.into_iter().enumerate() {
            println!("location {count}/{total}");
            match get_location2(id, &google_maps_client).await {
                Ok(location) => {
                    println!("location2 found");
                    locations.insert(id.to_string(), location);
                }
                Err(info) => {
                    println!("location2 not found");
                    blacklist.insert(id.to_string());
                    non_live_camera.insert(info);
                }
            }
        }
        write_hash_set(blacklist, "blacklist.txt.gz").await?;
        write_hash_set(non_live_camera, "non_live_camera.txt.gz").await?;
    }
    if locations.len() < current_count / 2 {
        let locations_len = locations.len();
        println!("new count of locations is too small: {locations_len} < {current_count} / 2");
        let err: Result<(), Box<dyn std::error::Error>> = Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "new count of locations is too small",
        )));
        return err;
    }
    write_geo(locations).await?;
    Ok(())
}
