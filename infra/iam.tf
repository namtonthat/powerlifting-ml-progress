## Github Actions
resource "aws_iam_user" "github_actions" {
  name = "github_actions_powerlifting"
}

data "aws_iam_policy_document" "github_actions_policy" {
  statement {
    sid = "S3ReadAccess"
    actions = [
      "s3:*",
      "s3-object-lambda:*"
    ]
    resources = [
      "arn:aws:s3:::${var.aws_s3_bucket}",
      "arn:aws:s3:::${var.aws_s3_bucket}/*"
    ]
  }
}

resource "aws_iam_policy" "github_actions_policy" {
  name        = "github_actions_powerlifting_policy"
  description = "Policy for github_actions to access S3 and ECR resources"
  policy      = data.aws_iam_policy_document.github_actions_policy.json
}

resource "aws_iam_user_policy_attachment" "github_actions_attach" {
  user       = aws_iam_user.github_actions.name
  policy_arn = aws_iam_policy.github_actions_policy.arn
}

resource "aws_iam_access_key" "github_actions_key" {
  user = aws_iam_user.github_actions.name
}
