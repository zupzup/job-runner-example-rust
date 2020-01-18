use futures::join;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use jobs::{BoxedJob, Job, JobContext, JobResult, JobRunner};
use log::{error, info};
use mobc::Pool;
use mobc_postgres::{tokio_postgres, PgConnectionManager};
use mobc_redis::RedisConnectionManager;
use std::convert::Infallible;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tokio_postgres::{Config, NoTls};

mod cache;
mod jobs;

const DUMMY_JOB_NAME: &str = "DUMMY JOB";
const DUMMY_JOB_SYNC_CACHE_NAME: &str = "DUMMY_JOB_LAST_RUN";
const SECOND_JOB_NAME: &str = "SECOND JOB";
const SECOND_JOB_SYNC_CACHE_NAME: &str = "SECOND_JOB_LAST_RUN";

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync + 'static>>;
type DBPool = Pool<PgConnectionManager<NoTls>>;
type RedisPool = Pool<RedisConnectionManager>;

async fn hello(_: Request<Body>) -> Result<Response<Body>> {
    Ok(Response::new(Body::from("Hello World!")))
}

#[derive(Clone)]
pub struct DummyJob;

impl Job for DummyJob {
    fn run(&self, ctx: &JobContext) -> JobResult {
        info!("Running DummyJob...");
        let ctx_clone = ctx.clone();
        let fut = async move {
            let db = ctx_clone.db_pool.get().await?;
            db.execute("SELECT 1", &[]).await?;
            info!("DummyJob finished!");
            Ok(())
        };
        Box::pin(fut)
    }
    fn get_interval(&self) -> Duration {
        Duration::from_secs(15)
    }
    fn get_name(&self) -> &'static str {
        DUMMY_JOB_NAME
    }
    fn get_sync_key(&self) -> &'static str {
        DUMMY_JOB_SYNC_CACHE_NAME
    }
    fn box_clone(&self) -> BoxedJob {
        Box::new((*self).clone())
    }
}

#[derive(Clone)]
pub struct SecondJob;

impl Job for SecondJob {
    fn run(&self, _ctx: &JobContext) -> JobResult {
        info!("Running SecondJob...");
        let fut = async move {
            info!("just print some stuff here...");
            info!("SecondJob finished!");
            Ok(())
        };
        Box::pin(fut)
    }
    fn get_interval(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn get_name(&self) -> &'static str {
        SECOND_JOB_NAME
    }
    fn get_sync_key(&self) -> &'static str {
        SECOND_JOB_SYNC_CACHE_NAME
    }
    fn box_clone(&self) -> BoxedJob {
        Box::new((*self).clone())
    }
}

pub fn init_logging() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{ts} {lvl:<5} [{thread:>25.25}] {msg}",
                ts = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                lvl = record.level(),
                thread = std::thread::current().name().unwrap_or("main"),
                msg = message
            ))
        })
        .level(log::LevelFilter::Off)
        .level_for("job_runner_example_rust", log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

pub async fn db_connect() -> Result<DBPool> {
    let config = Config::from_str("postgres://postgres@127.0.0.1:5432")?;
    info!("Connecting to the database...");
    let manager = PgConnectionManager::new(config, NoTls);
    Ok(Pool::builder().max_open(20).build(manager))
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let redis_pool = cache::connect().await.expect("Redis pool can be created");
    let db_pool = db_connect().await.expect("Database pool can be created.");

    let job_context = JobContext {
        db_pool: db_pool.clone(),
    };
    let dummy_job = DummyJob {};
    let second_job = SecondJob {};
    let job_runner = JobRunner::new(
        redis_pool.clone(),
        job_context,
        vec![
            Box::new(dummy_job) as BoxedJob,
            Box::new(second_job) as BoxedJob,
        ],
    );

    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(hello)) });

    let addr = ([127, 0, 0, 1], 3000).into();

    let server = Server::bind(&addr).serve(make_svc);

    info!("Listening on http://{}", addr);

    let res = join!(server, job_runner.run_jobs());
    res.0.map_err(|e| {
        error!("server crashed");
        e
    })?;

    Ok(())
}
