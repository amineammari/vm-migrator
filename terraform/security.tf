module "security_groups" {
  source = "./modules/security_groups"

  security_group_name = var.security_group_name
}
