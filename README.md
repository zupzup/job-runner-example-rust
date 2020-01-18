# job-runner-example-rust

An example async job runner, which synchronizes via redis.

# Run Example

Start Redis via Docker

```bash
sudo docker run -p 6379:6379 redis:5.0
```

Start Postgres Server via Docker

```bash
sudo docker run -p 5432:5432 -d postgres:9.6
```

Start Server

```bash
cargo run
```

Then go to `http://localhost:3000/` to test if the web server works. In the log you should see log output of the running jobs.

You can also start another instance (change the port) and check, that the jobs are only run once, synchronized via redis.
