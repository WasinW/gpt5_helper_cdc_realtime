cat > branch_oop_refactor_v2_fix.sh <<'SH'
analytics:
  tables:
    - name: ms_member
      module: { class: "com.domain.member.modules.MsMemberTransform", jar: "gs://dp_framework/business-domain/member/jars/member-analytics-mods.jar" }
EOF

cat > business-domains/member/resources/mappings/structure.yaml <<'EOF'
s_loy_program:
  program_id: program_id
  member_id: member_id
EOF

cat > business-domains/member/resources/reconcile_mapping.yaml <<'EOF'
reconcile:
  mapping:
    raw:
      s_loy_program:
        gcs_to_s3:
          program_id: PROG_ID
          member_id: CUST_ID
    refined:
      member_profile:
        gcs_to_s3:
          member_id: CUST_ID
          tier: LVL
    analytics:
      ms_member:
        gcs_to_s3:
          member_id: CUST_ID
          last_purchase_dt: LAST_TXN_DATE
EOF

cat > business-domains/member/resources/quality_rules.yaml <<'EOF'
rules:
  - id: not_null_member_id
    type: not_null
    columns: [member_id]
EOF

cat > docs/SOLUTION_DESIGN.md <<'EOF'
# Solution Design (OOP split + Config-driven + Reconcile mapping)
- Split framework vs domain, stages per feature, config-driven DAG per zone/table.
- Reconcile supports field mapping (GCS↔S3) per table and corrective transform on mismatch.
EOF

cat > docs/INVENTORY.md <<'EOF'
# Inventory (updated)
- Framework core/stages/connectors added
- Member domain adapters/modules/wiring/config added
- See Terraform for infra
EOF

# 4) build.sbt augment (idempotent)
if ! grep -q "cdc-oop-refactor" build.sbt 2>/dev/null; then
  cat >> build.sbt <<'EOF'
/* cdc-oop-refactor */
ThisBuild / scalaVersion := "2.12.18"
lazy val framework = (project in file("framework")).settings(name := "cdc-framework")
lazy val member    = (project in file("business-domains/member")).dependsOn(framework).settings(name := "member-domain")
EOF
fi

# 5) commit & push
git add -A
if git diff --cached --quiet; then
  echo "(nothing to commit)"
else
  git commit -m "feat(oop): split framework vs domain; stages; transform module; reconcile field mapping; config-driven multi-table"
fi

git push -u origin "$BRANCH"
echo "✅ Pushed $BRANCH. Open PR to review."
SH
