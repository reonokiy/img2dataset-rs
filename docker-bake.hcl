variable "VERSION" {
  default = "latest"
}

variable "REGISTRY" {
  default = "ghcr.io/reonokiy"
}

variable "IMAGE_NAME" {
  default = "img2dataset-rs"
}

group "default" {
  targets = ["img2dataset-rs"]
}

group "release" {
  targets = ["img2dataset-rs"]
}

target "img2dataset-rs" {
  context = "."
  dockerfile = "Dockerfile"
  tags = [
    "${REGISTRY}/${IMAGE_NAME}:${VERSION}",
    "${REGISTRY}/${IMAGE_NAME}:latest"
  ]
  platforms = [
    "linux/amd64",
    "linux/arm64"
  ]
  cache-from = [
    "type=gha"
  ]
  cache-to = [
    "type=gha,mode=max"
  ]
}
