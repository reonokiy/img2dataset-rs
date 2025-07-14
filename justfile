# Justfile for img2dataset-rs local development and release management

# Default recipe
default:
    @just --list

# Build in debug mode
build:
    cargo build

# Build in release mode
build-release:
    cargo build --release

# Run tests
test:
    cargo test

# Run clippy linter
lint:
    cargo clippy -- -D warnings

# Format code
fmt:
    cargo fmt

# Check formatting without making changes
fmt-check:
    cargo fmt -- --check

# Run all checks (test, lint, format check)
check: test lint fmt-check

# Clean build artifacts
clean:
    cargo clean

# Install binary locally
install:
    cargo install --path .

# Create a new release tag and push to trigger GitHub Actions
release version:
    #!/usr/bin/env bash
    set -euo pipefail
    
    # Validate version format
    if [[ ! "{{version}}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Error: Version must be in format vX.Y.Z (e.g., v1.0.0)"
        exit 1
    fi
    
    # Check if tag already exists
    if git rev-parse "{{version}}" >/dev/null 2>&1; then
        echo "Error: Tag {{version}} already exists"
        exit 1
    fi
    
    # Ensure working directory is clean
    if [[ -n $(git status --porcelain) ]]; then
        echo "Error: Working directory is not clean. Commit or stash changes first."
        exit 1
    fi
    
    # Update Cargo.toml version if needed
    echo "Updating version in Cargo.toml..."
    CARGO_VERSION=$(echo "{{version}}" | sed 's/^v//')
    sed -i.bak "s/^version = .*/version = \"$CARGO_VERSION\"/" Cargo.toml
    rm Cargo.toml.bak
    
    # Update Cargo.lock to reflect the version change
    echo "Updating Cargo.lock..."
    cargo check --quiet
    
    # Commit version update
    git add Cargo.toml Cargo.lock
    git commit -m "chore: bump version to {{version}}"
    
    # Create and push tag
    echo "Creating tag {{version}}..."
    git tag -a "{{version}}" -m "Release {{version}}"
    
    echo "Pushing to origin..."
    git push origin main
    git push origin "{{version}}"
    
    echo "Release {{version}} triggered! Check GitHub Actions for build progress."

# Build for all release targets locally (requires cross)
build-all-targets:
    #!/usr/bin/env bash
    set -euo pipefail
    
    # Install cross if not available
    if ! command -v cross &> /dev/null; then
        echo "Installing cross..."
        cargo install cross
    fi
    
    targets=(
        "x86_64-unknown-linux-gnu"
        "x86_64-unknown-linux-musl"
        "aarch64-unknown-linux-gnu"
        "aarch64-unknown-linux-musl"
        "x86_64-pc-windows-gnu"
        "x86_64-apple-darwin"
        "aarch64-apple-darwin"
    )
    
    for target in "${targets[@]}"; do
        echo "Building for $target..."
        if [[ "$target" == *"apple"* ]]; then
            # Use cargo for Apple targets (cross doesn't support them well)
            cargo build --release --target "$target" || echo "Skipped $target (not available)"
        else
            cross build --release --target "$target"
        fi
    done
    
    echo "All builds completed!"

# Generate changelog between two git references
changelog from="HEAD~10" to="HEAD":
    git log --pretty=format:"- %s" {{from}}..{{to}}

# Dry run release (create tag locally without pushing)
release-dry version:
    #!/usr/bin/env bash
    set -euo pipefail
    
    if [[ ! "{{version}}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Error: Version must be in format vX.Y.Z (e.g., v1.0.0)"
        exit 1
    fi
    
    echo "DRY RUN: Would create tag {{version}}"
    echo "Current HEAD: $(git rev-parse HEAD)"
    echo "Would update Cargo.toml version to: $(echo "{{version}}" | sed 's/^v//')"
    echo "Run 'just release {{version}}' to actually create the release"
