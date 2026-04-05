variable "run_prefix" {
  description = "Unique per-run prefix — all resource names are scoped to this."
  type        = string
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "test"
}

variable "enable_alerts" {
  description = "Whether to provision the SNS/SQS alert pipeline."
  type        = bool
  default     = true
}
