#!/usr/bin/env bash
set -euo pipefail
red(){ printf "\e[31m✗ %s\e[0m\n" "$*"; }
green(){ printf "\e[32m✓ %s\e[0m\n" "$*"; }
check_grep(){ local f="$1"; shift; local p="$*"; if grep -qE "$p" "$f"; then green "match: $f / $p"; else red "no match: $f / $p"; fi }

echo "== Branch =="
git rev-parse --abbrev-ref HEAD

echo "== Files exist =="
for f in \
  framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/BqReadStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/FieldMapStage.scala \
  business-domains/member/resources/pipeline_config.yaml \
  business-domains/member/resources/quality_rules.yaml \
  business-domains/member/resources/mappings/structure.yaml \
  business-domains/member/resources/reconcile_mapping.yaml
do
  [[ -f "$f" ]] && green "exists: $f" || red "missing: $f"
done

echo "== Config sanity =="
yml=business-domains/member/resources/pipeline_config.yaml
check_grep "$yml" '^datasets:[[:space:]]*$'
check_grep "$yml" 'raw:[[:space:]]*"member_raw"'
check_grep "$yml" 'structure:[[:space:]]*"member_structure"'
check_grep "$yml" 'refined:[[:space:]]*"member_refined"'
check_grep "$yml" 'analytics:[[:space:]]*"member_analytics"'
check_grep "$yml" 'pubsub:'
check_grep "$yml" 'routing_attribute:[[:space:]]*"table_name"'
check_grep "$yml" 'raw:[[:space:]]*\n[[:space:]]*tables:.*s_loy_program.*s_org_ext.*s_loy_tier.*s_loy_mem_tier.*s_loy_member.*s_contact.*s_con_addr.*s_addr_per.*s_loy_card.*s_loy_member_xm.*s_user.*cx_supp_type.*cx_com_channel'
check_grep "$yml" 'structure:[[:space:]]*\n[[:space:]]*tables:'
check_grep "$yml" 'refined:[[:space:]]*\n[[:space:]]*tables:.*member_profile'
check_grep "$yml" 'analytics:[[:space:]]*\n[[:space:]]*tables:.*ms_member'

echo "== Runner & stages sanity =="
check_grep framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala 'case "raw_ingest"'
check_grep framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala 'case "raw_to_structure"'
check_grep framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala 'case class Notification\\(accountId:String, eventType:String, table:String, raw:String, attrs: Map\\[String,String\\]\\)'
check_grep framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala 'class MessageToRawStage\\('
check_grep framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala 'DynamicDestinations'

echo "== build.sbt deps =="
check_grep build.sbt 'beam-sdks-java-io-google-cloud-platform'
check_grep build.sbt 'snakeyaml'
check_grep build.sbt 'jackson-module-scala'

echo "== Domain layout =="
if ls business-domains/member/src/main/scala | grep -vqE 'modules'; then red "unexpected dirs under domain/member/src/main/scala"; else green "domain code contains only modules (as expected)"; fi

echo "== Try compile (optional) =="
if command -v sbt >/dev/null; then sbt -v -Dsbt.supershell=false "compile"; else echo "skip sbt compile (sbt not found)"; fi
