module "base_project" {
  source = "./modules/base_project"

  project_name = var.project_name
}

module "network" {
  source = "./modules/network"

  private_network_name = var.private_network_name
  private_subnet_name  = var.private_subnet_name
  private_subnet_cidr  = var.private_subnet_cidr
  router_name          = var.router_name
  external_network_id  = var.external_network_id
}
