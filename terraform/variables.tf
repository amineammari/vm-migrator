variable "auth_url" {
  description = "OpenStack identity URL"
  type        = string
}

variable "username" {
  description = "OpenStack username"
  type        = string
}

variable "password" {
  description = "OpenStack password"
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "OpenStack project/tenant"
  type        = string
}

variable "domain_name" {
  description = "OpenStack user/project domain"
  type        = string
  default     = "Default"
}

variable "region" {
  description = "OpenStack region"
  type        = string
  default     = "RegionOne"
}

variable "external_network_id" {
  description = "Public/external network id"
  type        = string
}

variable "private_network_name" {
  description = "Tenant private network name"
  type        = string
  default     = "migrator-private"
}

variable "private_subnet_name" {
  description = "Tenant private subnet name"
  type        = string
  default     = "migrator-subnet"
}

variable "private_subnet_cidr" {
  description = "Private subnet CIDR"
  type        = string
  default     = "10.30.0.0/24"
}

variable "router_name" {
  description = "Router name"
  type        = string
  default     = "migrator-router"
}

variable "security_group_name" {
  description = "Security group name"
  type        = string
  default     = "migrator-default-sg"
}
