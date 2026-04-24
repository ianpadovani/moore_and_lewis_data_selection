#!/usr/bin/env bash
# Launch an EC2 instance to run Moore & Lewis data selection.
# Input corpora are read directly from S3 via s3fs — no pre-download.
# Results and logs are uploaded to S3 on completion; the instance terminates itself.
#
# Usage:
#   ./scripts/launch_ec2.sh \
#     --iam-profile <name> \
#     --s3-general  s3://bucket/path/to/general/ \
#     --s3-specific s3://bucket/path/to/specific/ \
#     --s3-output   s3://bucket/path/to/output/ \
#     --src-lang    en \
#     --tgt-lang    fr \
#     [--src-col <col>] [--tgt-col <col>] \
#     [--rank-src true] [--rank-tgt true] \
#     [--ami-id ami-xxx] [--instance-type r5.2xlarge] \
#     [--region eu-west-1] [--s3-bucket <bucket>] \
#     [--disk-size 500] [--key-name mykey] [--security-group sg-xxx] \
#     [--name ml-data-select]
#
# Requires: aws CLI with credentials that can launch EC2 and read/write S3.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Load defaults from .env if present (gitignored — copy from .env.example)
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -o allexport
    source "$SCRIPT_DIR/.env"
    set +o allexport
fi

IAM_PROFILE="${IAM_PROFILE:-}"
S3_GENERAL=""
S3_SPECIFIC=""
S3_OUTPUT=""
SRC_LANG=""
TGT_LANG=""
SRC_COL=""
TGT_COL=""
RANK_SRC="true"
RANK_TGT="true"
AMI_ID=""
INSTANCE_NAME="ml-data-select"
INSTANCE_TYPE="r5.2xlarge"
DISK_SIZE=500
KEY_NAME=""
SECURITY_GROUP=""
S3_BUCKET="${S3_BUCKET:-}"
REGION="${REGION:-eu-west-1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --iam-profile)    IAM_PROFILE="$2";    shift 2 ;;
        --s3-general)     S3_GENERAL="$2";     shift 2 ;;
        --s3-specific)    S3_SPECIFIC="$2";    shift 2 ;;
        --s3-output)      S3_OUTPUT="$2";      shift 2 ;;
        --src-lang)       SRC_LANG="$2";       shift 2 ;;
        --tgt-lang)       TGT_LANG="$2";       shift 2 ;;
        --src-col)        SRC_COL="$2";        shift 2 ;;
        --tgt-col)        TGT_COL="$2";        shift 2 ;;
        --rank-src)       RANK_SRC="$2";       shift 2 ;;
        --rank-tgt)       RANK_TGT="$2";       shift 2 ;;
        --ami-id)         AMI_ID="$2";         shift 2 ;;
        --name)           INSTANCE_NAME="$2";  shift 2 ;;
        --instance-type)  INSTANCE_TYPE="$2";  shift 2 ;;
        --disk-size)      DISK_SIZE="$2";      shift 2 ;;
        --region)         REGION="$2";         shift 2 ;;
        --s3-bucket)      S3_BUCKET="$2";      shift 2 ;;
        --key-name)       KEY_NAME="$2";       shift 2 ;;
        --security-group) SECURITY_GROUP="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

for flag in IAM_PROFILE S3_BUCKET S3_GENERAL S3_SPECIFIC S3_OUTPUT SRC_LANG TGT_LANG; do
    if [ -z "${!flag}" ]; then
        echo "ERROR: --$(echo "$flag" | tr '[:upper:]_' '[:lower:]-') is required."
        exit 1
    fi
done

# Default column names to language codes if not provided
SRC_COL="${SRC_COL:-$SRC_LANG}"
TGT_COL="${TGT_COL:-$TGT_LANG}"

TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
S3_CODE="s3://${S3_BUCKET}/ec2-jobs/${INSTANCE_NAME}_${TIMESTAMP}.tar.gz"
S3_LOGS="s3://${S3_BUCKET}/logs/moore-lewis"
OUTPUT_PREFIX="${S3_OUTPUT%/}"

# ── 1. Package code ───────────────────────────────────────────────────────────
echo "==> Packaging code"
TARBALL=$(mktemp --suffix=.tar.gz)
tar -czf "$TARBALL" \
    --exclude='./.env' \
    --exclude='./.venv' \
    --exclude='./bin' \
    --exclude='./__pycache__' \
    --exclude='./*.egg-info' \
    --exclude='./.git' \
    --exclude='./.pytest_cache' \
    --exclude='./.ruff_cache' \
    --exclude='./.claude' \
    --exclude='./.idea' \
    -C "$SCRIPT_DIR" .
echo "    Tarball: ${TARBALL} ($(du -sh "$TARBALL" | cut -f1))"

# ── 2. Upload code to S3 ──────────────────────────────────────────────────────
echo "==> Uploading code to ${S3_CODE}"
aws s3 cp "$TARBALL" "$S3_CODE" --region "$REGION"
rm -f "$TARBALL"

# ── 3. Resolve Amazon Linux 2023 AMI ─────────────────────────────────────────
if [ -n "$AMI_ID" ]; then
    echo "==> Using provided AMI: ${AMI_ID}"
else
    echo "==> Looking up latest Amazon Linux 2023 AMI in ${REGION}"
    AMI_ID=$(aws ec2 describe-images \
        --owners amazon \
        --filters \
            "Name=name,Values=al2023-ami-2023*-kernel-*-x86_64" \
            "Name=state,Values=available" \
            "Name=architecture,Values=x86_64" \
        --query "sort_by(Images, &CreationDate)[-1].ImageId" \
        --output text \
        --region "$REGION")
    if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
        echo "ERROR: Could not find Amazon Linux 2023 AMI automatically."
        echo "  Pass --ami-id ami-xxxxxxxxxxxxxxxxx to specify one explicitly."
        exit 1
    fi
    echo "    AMI: ${AMI_ID}"
fi

# ── 4. Substitute placeholders in userdata ────────────────────────────────────
USERDATA_TEMPLATE="${SCRIPT_DIR}/scripts/ec2_userdata.sh"
USERDATA_FILE=$(mktemp)
sed \
    -e "s|@@REGION@@|${REGION}|g" \
    -e "s|@@S3_CODE@@|${S3_CODE}|g" \
    -e "s|@@S3_BUCKET@@|${S3_BUCKET}|g" \
    -e "s|@@S3_LOGS@@|${S3_LOGS}|g" \
    -e "s|@@S3_GENERAL@@|${S3_GENERAL}|g" \
    -e "s|@@S3_SPECIFIC@@|${S3_SPECIFIC}|g" \
    -e "s|@@S3_OUTPUT@@|${OUTPUT_PREFIX}|g" \
    -e "s|@@INSTANCE_NAME@@|${INSTANCE_NAME}|g" \
    -e "s|@@SRC_LANG@@|${SRC_LANG}|g" \
    -e "s|@@TGT_LANG@@|${TGT_LANG}|g" \
    -e "s|@@SRC_COL@@|${SRC_COL}|g" \
    -e "s|@@TGT_COL@@|${TGT_COL}|g" \
    -e "s|@@RANK_SRC@@|${RANK_SRC}|g" \
    -e "s|@@RANK_TGT@@|${RANK_TGT}|g" \
    "$USERDATA_TEMPLATE" | gzip > "$USERDATA_FILE"

# ── 5. Launch EC2 instance ────────────────────────────────────────────────────
RUN_ARGS=(
    --image-id             "$AMI_ID"
    --instance-type        "$INSTANCE_TYPE"
    --region               "$REGION"
    --iam-instance-profile "Name=${IAM_PROFILE}"
    --user-data            "fileb://${USERDATA_FILE}"
    --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":${DISK_SIZE},\"VolumeType\":\"gp3\"}}]"
    --tag-specifications   "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME}}]"
    --metadata-options     "HttpTokens=required,HttpEndpoint=enabled"
    --instance-initiated-shutdown-behavior terminate
    --query                "Instances[0].InstanceId"
    --output               text
)

if [ -n "$KEY_NAME" ];       then RUN_ARGS+=(--key-name "$KEY_NAME"); fi
if [ -n "$SECURITY_GROUP" ]; then RUN_ARGS+=(--security-group-ids "$SECURITY_GROUP"); fi

echo "==> Launching ${INSTANCE_TYPE} in ${REGION}"
INSTANCE_ID=$(aws ec2 run-instances "${RUN_ARGS[@]}")
rm -f "$USERDATA_FILE"

echo ""
echo "Instance launched: ${INSTANCE_ID}"
echo ""
echo "Logs streamed to S3 every 5 minutes:"
echo "  ${S3_LOGS}/${INSTANCE_NAME}_${TIMESTAMP}.log"
echo ""
echo "Download log:"
echo "  aws s3 cp ${S3_LOGS}/${INSTANCE_NAME}_${TIMESTAMP}.log . --region ${REGION}"
echo ""
echo "Results uploaded to:"
echo "  ${OUTPUT_PREFIX}/"
echo ""
echo "Download results after completion:"
echo "  aws s3 sync ${OUTPUT_PREFIX}/ ./output/ --region ${REGION}"
echo ""
if [ -n "$KEY_NAME" ]; then
    echo "Tail log (once running):"
    echo "  PUBLIC_IP=\$(aws ec2 describe-instances --instance-ids ${INSTANCE_ID} --region ${REGION} --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
    echo "  ssh ec2-user@\$PUBLIC_IP tail -f /var/log/pipeline.log"
fi
