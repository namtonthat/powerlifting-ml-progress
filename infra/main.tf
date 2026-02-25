terraform {
  backend "s3" {
    bucket  = "powerlifting-ml-progress"
    key     = "terraform/state.tfstate"
    region  = "ap-southeast-2"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
}
