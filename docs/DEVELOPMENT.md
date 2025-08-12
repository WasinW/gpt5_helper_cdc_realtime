# Development

## Prereqs
- JDK 17
- sbt 1.11.x
- Google Cloud & AWS credentials if exercising BQ/S3 code

## Build
```bash
sbt compile
```

## Run (membership example)
Integration requires real cloud services; for local dev, use `mock_s3_dir` in config to point to local snapshot files:
```
aws_snapshot_s3: s3://bucket/path        # real
# or
mock_s3_dir: /absolute/path/to/snapshots  # local dev
```

## Testing
Add unit tests under `framework/src/test/scala` and `member-pipeline/src/test/scala`.

## Coding conventions
- Scala 2.12, avoid `scala.jdk.CollectionConverters` (use `JavaConverters`).
- All Beam stages should be **pure** in outputs and use `audit(String)` for side effects.
