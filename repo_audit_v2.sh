#!/usr/bin/env bash
set -euo pipefail
red(){ printf "\e[31m✗ %s\e[0m\n" "$*"; }
green(){ printf "\e[32m✓ %s\e[0m\n" "$*"; }

check_exist(){ [[ -f "$1" ]] && green "exists: $1" || red "missing: $1"; }
check_line(){ local f="$1" p="$2"; if grep -qF "$p" "$f"; then green "match: $f / $p"; else red "no match: $f / $p"; fi }

echo "== Branch =="; git rev-parse --abbrev-ref HEAD

echo "== Files exist =="
check_exist framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala
check_exist framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala
check_exist framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala
check_exist framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala
check_exist framework/src/main/scala/com/analytics/framework/pipeline/stages/BqReadStage.scala
check_exist framework/src/main/scala/com/analytics/framework/pipeline/stages/FieldMapStage.scala
check_exist business-domains/member/resources/pipeline_config.yaml
check_exist business-domains/member/resources/quality_rules.yaml
check_exist business-domains/member/resources/mappings/structure.yaml
check_exist business-domains/member/resources/reconcile_mapping.yaml

echo "== Config sanity =="
yml=business-domains/member/resources/pipeline_config.yaml
for s in \
  "datasets:" \
  'raw: "member_raw"' \
  'structure: "member_structure"' \
  'refined: "member_refined"' \
  'analytics: "member_analytics"' \
  "pubsub:" \
  'routing_attribute: "table_name"'
do check_line "$yml" "$s"; done

# ตารางใน raw/structure: เช็กว่ามีชื่อครบในไฟล์
raw_tables=(s_loy_program s_org_ext s_loy_tier s_loy_mem_tier s_loy_member s_contact s_con_addr s_addr_per s_loy_card s_loy_member_xm s_user cx_supp_type cx_com_channel)
echo "-- raw tables present? --"
for t in "${raw_tables[@]}"; do
  if grep -qF "$t" "$yml"; then green "raw contains: $t"; else red "raw missing: $t"; fi
done

echo "-- structure tables present? --"
for t in "${raw_tables[@]}"; do
  if grep -qF "$t" "$yml"; then green "structure contains: $t"; else red "structure missing: $t"; fi
done

echo "-- refined/analytics tables present? --"
check_line "$yml" "name: member_profile"
check_line "$yml" "name: ms_member"

echo "== Runner & stages sanity =="
check_line framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala 'case "raw_ingest"'
check_line framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala 'case "raw_to_structure"'
check_line framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala 'case class Notification('
check_line framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala 'attrs: Map[String,String]'
check_line framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala 'class MessageToRawStage('
check_line framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala 'DynamicDestinations'

echo "== build.sbt deps =="
for s in "beam-sdks-java-io-google-cloud-platform" "snakeyaml" "jackson-module-scala"; do
  check_line build.sbt "$s"
done

echo "== Domain layout =="
if [ -d business-domains/member/src/main/scala ]; then
  if ls business-domains/member/src/main/scala | grep -vqE '^modules$'; then
    red "unexpected dirs under domain/member/src/main/scala"
  else green "domain code contains only modules (as expected)"; fi
else
  green "domain code contains only configs (as expected)"
fi

echo "== Try compile (optional) =="
if command -v sbt >/dev/null; then sbt -v -Dsbt.supershell=false "compile"; else echo "skip sbt compile (sbt not found)"; fi
