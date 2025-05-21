locals {
  region = "us-east-1"
  global_tags = {
    owner = "marcos.foglino@caylent.com"
  }
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=5.71.0"
    }
  }
}

provider "aws" {
  region  = local.region
  profile = "caylent-dev-test"
  default_tags {
    tags = local.global_tags
  }
}
