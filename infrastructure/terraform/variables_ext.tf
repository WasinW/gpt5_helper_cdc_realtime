variable "project_tag" { type = string  default = "new_data_plateform" }

variable "framework_bucket"         { type = string  default = "dp_framework" }
variable "data_secure_bucket"       { type = string  default = "dp_data_secure" }
variable "data_analytics_bucket"    { type = string  default = "dp_data_analytics" }
variable "data_secure_dlq_bucket"   { type = string  default = "dp_data_secure_dlq" }
variable "data_analytics_dlq_bucket"{ type = string  default = "dp_data_analytics_dlq" }
