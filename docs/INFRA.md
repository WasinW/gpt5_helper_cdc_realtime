# Infrastructure (dev)
- Buckets: demo-the-1-staging / demo-the-1-audit / demo-the-1-config / demo-the-1-dlq
- Pub/Sub: member-events-create/update (+ subs & dlq-sub)
- BigQuery datasets: member_raw|member_structure|member_refined|member_analytics
- Backend: demo-the-1-tf-state / cdc/
Usage:
  cd infrastructure/terraform
  terraform init
  terraform apply -var-file=./params/env.dev.tfvars
