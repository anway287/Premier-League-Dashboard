output "raw_data_bucket" {
  value = aws_s3_bucket.raw_data.bucket
}

output "stats_table" {
  value = aws_dynamodb_table.stats.name
}

output "events_queue_url" {
  value = var.enable_alerts ? aws_sqs_queue.events[0].url : ""
}

output "alerts_topic_arn" {
  value = var.enable_alerts ? aws_sns_topic.alerts[0].arn : ""
}
