variable "project_id" {}
variable "region" {}
variable "labels" { type = map(string) }
variable "network_name" { default = "cdc-vpc" }
variable "subnetwork_name" { default = "cdc-subnet" }

resource "google_compute_network" "vpc" {
  name                    = var.network_name
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.subnetwork_name}-${var.region}"
  ip_cidr_range = "10.10.0.0/20"
  region        = var.region
  project       = var.project_id
  network       = google_compute_network.vpc.id
  private_ip_google_access = true
}

resource "google_compute_router" "router" {
  name    = "cdc-router"
  region  = var.region
  network = google_compute_network.vpc.name
}

resource "google_compute_router_nat" "nat" {
  name                               = "cdc-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"
  subnetwork {
    name                    = google_compute_subnetwork.subnet.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }
}

output "subnetwork_self_link" { value = google_compute_subnetwork.subnet.self_link }
