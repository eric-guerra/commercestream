terraform {
  required_version = ">= 1.6"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.40" }
    helm = { source = "hashicorp/helm", version = "~> 2.12" }
  }
  backend "s3" {
    bucket         = "commercestream-tfstate"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "commercestream-tf-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = { Environment = var.env, Owner = "platform", CostCenter = "commerce-data", ManagedBy = "terraform" } }
}

variable "env" { type = string }
variable "region" {
  type    = string
  default = "us-east-1"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.5"

  name               = "commercestream-${var.env}"
  cidr               = "10.60.0.0/16"
  azs                = ["${var.region}a", "${var.region}b", "${var.region}c"]
  private_subnets    = ["10.60.0.0/19", "10.60.32.0/19", "10.60.64.0/19"]
  public_subnets     = ["10.60.96.0/22", "10.60.100.0/22", "10.60.104.0/22"]
  enable_nat_gateway = true
  single_nat_gateway = var.env != "prod"

  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  cluster_name                   = "commercestream-${var.env}"
  cluster_version                = "1.29"
  vpc_id                         = module.vpc.vpc_id
  subnet_ids                     = module.vpc.private_subnets
  cluster_endpoint_public_access = false
  enable_irsa                    = true
  cluster_encryption_config      = { resources = ["secrets"] }

  eks_managed_node_groups = {
    # Brokers get their own nodes: predictable disk/network, no noisy neighbours.
    kafka = {
      instance_types = ["r6i.xlarge"]
      min_size       = 3
      max_size       = 6
      desired_size   = 3
      labels         = { workload = "kafka" }
      taints         = [{ key = "workload", value = "kafka", effect = "NO_SCHEDULE" }]
    }
    apps = {
      instance_types = ["m6i.large"]
      min_size       = 2
      max_size       = 12
      desired_size   = 3
      labels         = { workload = "apps" }
    }
  }
}

# Cluster add-ons the manifests depend on, installed once per cluster; apps come from Argo CD.
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "oci://quay.io/strimzi-helm"
  chart            = "strimzi-kafka-operator"
  version          = "0.41.0"
  namespace        = "kafka"
  create_namespace = true
}

resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.14.0"
  namespace        = "keda"
  create_namespace = true
}

resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "6.7.11"
  namespace        = "argocd"
  create_namespace = true
}

resource "helm_release" "otel_collector" {
  name             = "otel-collector"
  repository       = "https://open-telemetry.github.io/opentelemetry-helm-charts"
  chart            = "opentelemetry-collector"
  version          = "0.90.1"
  namespace        = "observability"
  create_namespace = true
  values           = [file("${path.module}/otel-collector-values.yaml")]
}

output "cluster_name" { value = module.eks.cluster_name }
