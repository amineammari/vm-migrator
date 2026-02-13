output "project_name" {
  value = module.base_project.project_name
}

output "network_id" {
  value = module.network.network_id
}

output "subnet_id" {
  value = module.network.subnet_id
}

output "router_id" {
  value = module.network.router_id
}

output "security_group_id" {
  value = module.security_groups.security_group_id
}
