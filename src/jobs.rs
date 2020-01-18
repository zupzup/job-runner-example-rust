use super::cache;
use crate::{DBPool, RedisPool, Result};
use chrono::Utc;
use futures::future::BoxFuture;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;

const JOB_CHECKING_INTERVAL_SECS: u64 = 30;

pub type BoxedJob = Box<dyn Job + Send + Sync>;
pub type JobResult = BoxFuture<'static, Result<()>>;

#[derive(Clone)]
pub struct JobContext {
    pub db_pool: DBPool,
}

pub trait Job {
    fn run(&self, ctx: &JobContext) -> JobResult;
    fn get_interval(&self) -> Duration;
    fn get_name(&self) -> &'static str;
    fn get_sync_key(&self) -> &'static str;
    fn box_clone(&self) -> BoxedJob;
}

impl Clone for Box<dyn Job> {
    fn clone(&self) -> Box<dyn Job> {
        self.box_clone()
    }
}

pub struct JobRunner {
    jobs: Vec<BoxedJob>,
    redis_pool: RedisPool,
    job_context: JobContext,
}

impl JobRunner {
    pub fn new(redis_pool: RedisPool, job_context: JobContext, jobs: Vec<BoxedJob>) -> JobRunner {
        JobRunner {
            jobs,
            redis_pool,
            job_context,
        }
    }

    pub async fn run_jobs(self) -> Result<()> {
        self.announce_jobs();
        let mut job_interval =
            tokio::time::interval(Duration::from_secs(JOB_CHECKING_INTERVAL_SECS));
        let arc_jobs = Arc::new(&self.jobs);
        loop {
            job_interval.tick().await;
            match self.check_and_run_jobs(&arc_jobs).await {
                Ok(_) => (),
                Err(e) => error!("Could not check and run Jobs: {}", e),
            };
        }
    }

    async fn check_and_run_jobs(&self, arc_jobs: &Arc<&Vec<BoxedJob>>) -> Result<()> {
        let jobs = arc_jobs.clone();
        for job in jobs.iter() {
            match self.check_and_run_job(job, &self.redis_pool).await {
                Ok(_) => (),
                Err(e) => error!("Error during Job run: {}", e),
            };
        }
        Ok(())
    }

    async fn check_and_run_job(&self, job: &BoxedJob, redis_pool: &RedisPool) -> Result<()> {
        let now = Utc::now().timestamp() as u64;
        let j = job.box_clone();
        let job_context_clone = self.job_context.clone();
        match cache::get_str(&redis_pool, job.get_sync_key()).await {
            Ok(v) => {
                let last_run = v.parse::<u64>()?;
                if now > job.get_interval().as_secs() + last_run {
                    self.set_last_run(now, &redis_pool, job.get_sync_key())
                        .await;
                    self.run_job(j, job_context_clone);
                }
            }
            Err(_) => {
                self.set_last_run(now, &redis_pool, job.get_sync_key())
                    .await;
                self.run_job(j, job_context_clone);
            }
        };
        Ok(())
    }

    fn run_job(&self, job: BoxedJob, job_context: JobContext) {
        let job_name = job.get_name();
        tokio::spawn(async move {
            info!("Starting Job {}...", job_name);
            match job.run(&job_context).await {
                Ok(_) => info!("Job {} finished successfully", job_name),
                Err(e) => error!("Job {} finished with error: {}", job_name, e),
            };
        });
    }

    async fn set_last_run(&self, now: u64, redis_pool: &RedisPool, job_key: &str) {
        match cache::set_str(&redis_pool, job_key, &now.to_string(), 0).await {
            Ok(_) => (),
            Err(_) => error!("Could not set Last Run Time"),
        };
    }

    fn announce_jobs(&self) {
        for job in &self.jobs {
            info!(
                "Registered Job {} with Interval: {:?}",
                job.get_name(),
                job.get_interval()
            );
        }
    }
}
