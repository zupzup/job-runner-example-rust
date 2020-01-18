use crate::{RedisPool, Result};
use mobc::Pool;
use mobc_redis::redis::{cmd, FromRedisValue, Value};
use mobc_redis::{redis, Connection, RedisConnectionManager};

pub async fn connect() -> Result<RedisPool> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let manager = RedisConnectionManager::new(client);
    Ok(Pool::builder().build(manager))
}

pub async fn set_str(pool: &RedisPool, key: &str, value: &str, ttl_seconds: usize) -> Result<()> {
    let mut con = pool.get().await?;
    cmd("SET")
        .arg(key)
        .arg(value)
        .query_async::<_, Value>(&mut con as &mut Connection)
        .await?;
    if ttl_seconds > 0 {
        cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query_async::<_, Value>(&mut con as &mut Connection)
            .await?;
    }
    Ok(())
}

pub async fn get_str(pool: &RedisPool, key: &str) -> Result<String> {
    let mut con = pool.get().await?;
    let value = cmd("GET")
        .arg(key)
        .query_async::<_, Value>(&mut con as &mut Connection)
        .await?;
    let parsed: String = FromRedisValue::from_redis_value(&value)?;
    Ok(parsed)
}
