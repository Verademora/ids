use clap::Parser;
use image_hasher::{HasherConfig, ImageHash};
use sqlx::{migrate::MigrateDatabase, sqlite::SqliteQueryResult, Row, Sqlite, SqlitePool};
use std::fs;
use std::path::PathBuf;
use std::result::Result;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(value_name = "PATH")]
    input: PathBuf,

    #[arg(short, long)]
    persist: bool,

    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
}

struct KnownDupe {
    filename: String,
    folder: i32,
}

async fn create_schema(db_url: &str) -> Result<SqliteQueryResult, sqlx::Error> {
    let pool = SqlitePool::connect(&db_url).await?;
    let qry = "CREATE TABLE IF NOT EXISTS images (
            filename    TEXT    PRIMARY KEY     NOT NULL,
            imagehash   TEXT    UNIQUE
        );";
    let result = sqlx::query(&qry).execute(&pool).await;
    pool.close().await;
    return result;
}

async fn fetch_matching(pool: &SqlitePool, hash: &ImageHash) -> Option<String> {
    let qry = "SELECT filename FROM images WHERE imagehash=$1";
    let result = sqlx::query(qry)
        .bind(hash.to_base64())
        .fetch_optional(pool)
        .await
        .unwrap_or_else(|_| {
            return None;
        });

    match result {
        None => None,
        Some(row) => Some(row.try_get(0).unwrap()),
    }
}

async fn pre_check(pool: &SqlitePool, path: &PathBuf) -> bool {
    let qry = "SELECT * FROM images WHERE filename=$1";
    let result = sqlx::query(&qry)
        .bind(path.file_name().unwrap().to_str())
        .fetch_optional(pool)
        .await
        .unwrap_or_else(|_| {
            return None;
        });

    match result {
        None => false,
        Some(row) => {
            if !row.is_empty() {
                return true;
            }
            false
        }
    }
}

async fn instert_db(pool: &SqlitePool, hash: &ImageHash, path: &PathBuf) {
    let qry = "INSERT INTO images (filename, imagehash) VALUES($1, $2)";
    let _result = sqlx::query(&qry)
        .bind(path.file_name().unwrap().to_str())
        .bind(hash.to_base64())
        .execute(pool)
        .await;
}

#[async_std::main]
async fn main() {
    let cli = Cli::parse();
    let db_url = String::from("sqlite://sqlite.db");
    let persist = cli.persist;
    if !persist {
        Sqlite::drop_database(&db_url).await.unwrap();
    }
    if !Sqlite::database_exists(&db_url).await.unwrap() {
        Sqlite::create_database(&db_url).await.unwrap();
        match create_schema(&db_url).await {
            Ok(_) => println!("Database created"),
            Err(e) => panic!("{}", e),
        }
    }
    let instances = SqlitePool::connect(&db_url).await.unwrap();
    let hasher = HasherConfig::new().to_hasher();

    let mut imgs = 0;
    let mut dups = 0;
    let mut dupelist: Vec<KnownDupe> = Vec::new();

    if cli.input.is_dir() {
        for entry in fs::read_dir(&cli.input).unwrap() {
            let mut input = cli.input.to_owned();
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                if cli.persist & pre_check(&instances, &path).await {
                    imgs += 1;
                    continue;
                }
                if let Ok(image) = image::open(&path) {
                    let hash = hasher.hash_image(&image);

                    match fetch_matching(&instances, &hash).await {
                        None => {
                            instert_db(&instances, &hash, &path).await;
                        }
                        Some(row) => {
                            let mut flag = false;
                            for dupe in &dupelist {
                                if dupe.filename == row {
                                    flag = true;
                                    input.push(dupe.folder.to_string());
                                    let mut copy = input.to_owned();
                                    copy.push(&path.file_name().unwrap());
                                    fs::copy(&path, &copy).unwrap();
                                }
                            }
                            if !flag {
                                dups += 1;
                                let dupe = KnownDupe {
                                    filename: row.to_string(),
                                    folder: dups,
                                };
                                dupelist.push(dupe);
                                let mut img1 = input.to_owned();
                                img1.push(&row);
                                input.push(dups.to_string());
                                println!(
                                    "Duplicate found. Creating dir {}",
                                    &input.to_str().unwrap()
                                );
                                match fs::create_dir(&input) {
                                    Ok(_) => {
                                        let mut copy1 = input.to_owned();
                                        copy1.push(&row);
                                        let mut copy2 = input.to_owned();
                                        copy2.push(&path.file_name().unwrap());
                                        fs::copy(&img1, &copy1).unwrap();
                                        fs::copy(&path, &copy2).unwrap();
                                        println!(
                                            "Copied {} and {} to {}",
                                            &row,
                                            path.file_name().unwrap().to_str().unwrap(),
                                            input.to_str().unwrap()
                                        );
                                    }
                                    Err(e) => println!("{}", e),
                                }
                            }
                        }
                    }
                    imgs += 1;
                }
            }
        }
    }
    instances.close().await;
    println!(
        "Complete. Checked {} images. Found {} duplicates.",
        imgs, dups
    );
    if !persist {
        Sqlite::drop_database(&db_url).await.unwrap();
    }
}
