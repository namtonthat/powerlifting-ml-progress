variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_s3_bucket" {
  description = "Data to hold powerlifting-ml-progress"
  type        = string
  default     = "powerlifting-ml-progress"

}
