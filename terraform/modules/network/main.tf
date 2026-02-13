terraform {
  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 2.1"
    }
  }
}

variable "private_network_name" {
  type = string
}

variable "private_subnet_name" {
  type = string
}

variable "private_subnet_cidr" {
  type = string
}

variable "router_name" {
  type = string
}

variable "external_network_id" {
  type = string
}

resource "openstack_networking_network_v2" "private" {
  name           = var.private_network_name
  admin_state_up = true
}

resource "openstack_networking_subnet_v2" "private" {
  name            = var.private_subnet_name
  network_id      = openstack_networking_network_v2.private.id
  cidr            = var.private_subnet_cidr
  ip_version      = 4
  dns_nameservers = ["8.8.8.8", "1.1.1.1"]
}

resource "openstack_networking_router_v2" "router" {
  name                = var.router_name
  admin_state_up      = true
  external_network_id = var.external_network_id
}

resource "openstack_networking_router_interface_v2" "router_if" {
  router_id = openstack_networking_router_v2.router.id
  subnet_id = openstack_networking_subnet_v2.private.id
}

output "network_id" {
  value = openstack_networking_network_v2.private.id
}

output "subnet_id" {
  value = openstack_networking_subnet_v2.private.id
}

output "router_id" {
  value = openstack_networking_router_v2.router.id
}
