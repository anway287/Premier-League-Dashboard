locals {
  # Every resource name is prefixed with the run ID, guaranteeing
  # zero cross-test interference even when runs execute in parallel.
  prefix = var.run_prefix
  tags = {
    Environment = var.environment
    RunPrefix   = var.run_prefix
    ManagedBy   = "hermetic-test-framework"
  }
}

# ---------------------------------------------------------------------------
# S3 — Raw sports event storage
# Ingester drops JSON blobs here; processor reads from here.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "raw_data" {
  bucket        = "${local.prefix}-sports-raw"
  force_destroy = true   # hermetic: clean up completely on terraform destroy
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------
# DynamoDB — Processed stats (teams, players, standings)
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "stats" {
  name           = "${local.prefix}-sports-stats"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "pk"
  range_key      = "sk"
  tags           = local.tags

  attribute {
    name = "pk"   # e.g. "TEAM#ManCity" or "PLAYER#Haaland"
    type = "S"
  }

  attribute {
    name = "sk"   # e.g. "SEASON#2023-24#WEEK#28"
    type = "S"
  }

  # GSI for querying all stats for a given season week across teams
  global_secondary_index {
    name            = "sk-pk-index"
    hash_key        = "sk"
    range_key       = "pk"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }
}

# ---------------------------------------------------------------------------
# SQS — Game event queue
# Events (GoalScored, MatchStarted, MatchEnded …) flow through here.
# ---------------------------------------------------------------------------
resource "aws_sqs_queue" "events_dlq" {
  count = var.enable_alerts ? 1 : 0
  name  = "${local.prefix}-sports-events-dlq"
  tags  = local.tags
}

resource "aws_sqs_queue" "events" {
  count                      = var.enable_alerts ? 1 : 0
  name                       = "${local.prefix}-sports-events"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400   # 1 day — sufficient for test runs
  tags                       = local.tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.events_dlq[0].arn
    maxReceiveCount     = 3
  })
}

# ---------------------------------------------------------------------------
# SNS — Alerts topic (goal alerts, injury news, match result summaries)
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  count = var.enable_alerts ? 1 : 0
  name  = "${local.prefix}-sports-alerts"
  tags  = local.tags
}

# Wire SNS → SQS so tests can assert on received notifications via SQS poll
resource "aws_sns_topic_subscription" "alerts_to_sqs" {
  count     = var.enable_alerts ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.events[0].arn
}

resource "aws_sqs_queue_policy" "allow_sns" {
  count     = var.enable_alerts ? 1 : 0
  queue_url = aws_sqs_queue.events[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.events[0].arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_sns_topic.alerts[0].arn }
      }
    }]
  })
}
