output "raw_data_bucket" {
  description = "S3 bucket for raw sports event data."
  value       = module.sports_pipeline_env.raw_data_bucket
}

output "stats_table" {
  description = "DynamoDB table name for processed team/player stats."
  value       = module.sports_pipeline_env.stats_table
}

output "events_queue_url" {
  description = "SQS queue URL for the game-event stream."
  value       = module.sports_pipeline_env.events_queue_url
}

output "alerts_topic_arn" {
  description = "SNS topic ARN for game alerts."
  value       = module.sports_pipeline_env.alerts_topic_arn
}

output "run_prefix" {
  description = "Echo back the run prefix — used by tests to resolve resource names."
  value       = var.run_prefix
}
