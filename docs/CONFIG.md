# CONFIG
- `pubsub.topic_*` and `subscription_*` allow switching between create/update streams.
- `logs.audit_bucket` is a **gs://** URI and must exist before running pipelines.
- `api.token_secret_id` must exist in Secret Manager with a valid Bearer token.
