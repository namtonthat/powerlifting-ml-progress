output "github_actions_access_key_id" {
  description = "The access key ID for the github_actions IAM user"
  value       = aws_iam_access_key.github_actions_key.id
}

output "github_actions_secret_access_key" {
  description = "The secret access key for the github_actions IAM user"
  value       = aws_iam_access_key.github_actions_key.secret
  sensitive   = true
}
