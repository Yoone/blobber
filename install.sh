#!/bin/sh
set -e

# Blobber installer script
# Usage: curl -sSL https://raw.githubusercontent.com/Yoone/blobber/main/install.sh | sh
#
# Options (via environment variables):
#   VERSION       - specific version to install (default: latest)
#   INSTALL_DIR   - installation directory (default: /usr/local/bin or ~/.local/bin)

REPO="Yoone/blobber"
BINARY="blobber"

# Colors for output (disabled if not a terminal)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

info() {
    printf "${BLUE}==>${NC} %s\n" "$1"
}

success() {
    printf "${GREEN}==>${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}Warning:${NC} %s\n" "$1"
}

error() {
    printf "${RED}Error:${NC} %s\n" "$1" >&2
    exit 1
}

# Detect OS
detect_os() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$OS" in
        linux*)  OS="linux" ;;
        darwin*) OS="darwin" ;;
        *)       error "Unsupported operating system: $OS" ;;
    esac
    echo "$OS"
}

# Detect architecture
detect_arch() {
    ARCH=$(uname -m)
    case "$ARCH" in
        x86_64|amd64)   ARCH="amd64" ;;
        aarch64|arm64)  ARCH="arm64" ;;
        *)              error "Unsupported architecture: $ARCH" ;;
    esac
    echo "$ARCH"
}

# Get latest version from GitHub API
get_latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -sSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/'
    else
        error "curl or wget is required"
    fi
}

# Download file
download() {
    URL=$1
    OUTPUT=$2
    if command -v curl >/dev/null 2>&1; then
        curl -sSL "$URL" -o "$OUTPUT"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$URL" -O "$OUTPUT"
    else
        error "curl or wget is required"
    fi
}

# Verify checksum
verify_checksum() {
    FILE=$1
    CHECKSUM_FILE=$2
    EXPECTED=$(grep "$(basename "$FILE")" "$CHECKSUM_FILE" | awk '{print $1}')

    if command -v sha256sum >/dev/null 2>&1; then
        ACTUAL=$(sha256sum "$FILE" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        ACTUAL=$(shasum -a 256 "$FILE" | awk '{print $1}')
    else
        warn "sha256sum or shasum not found, skipping checksum verification"
        return 0
    fi

    if [ "$EXPECTED" != "$ACTUAL" ]; then
        error "Checksum verification failed!\nExpected: $EXPECTED\nActual:   $ACTUAL"
    fi
}

main() {
    OS=$(detect_os)
    ARCH=$(detect_arch)

    info "Detected OS: $OS, Architecture: $ARCH"

    # Get version
    if [ -z "$VERSION" ]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
        if [ -z "$VERSION" ]; then
            error "Failed to get latest version"
        fi
    fi

    # Remove 'v' prefix if present for filename
    VERSION_NUM="${VERSION#v}"

    info "Installing ${BINARY} ${VERSION}..."

    # Construct download URL
    FILENAME="${BINARY}_${VERSION_NUM}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}"
    CHECKSUM_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"

    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT

    # Download files
    info "Downloading ${FILENAME}..."
    download "$DOWNLOAD_URL" "$TMP_DIR/$FILENAME"

    info "Downloading checksums..."
    download "$CHECKSUM_URL" "$TMP_DIR/checksums.txt"

    # Verify checksum
    info "Verifying checksum..."
    verify_checksum "$TMP_DIR/$FILENAME" "$TMP_DIR/checksums.txt"

    # Extract
    info "Extracting..."
    tar -xzf "$TMP_DIR/$FILENAME" -C "$TMP_DIR"

    # Determine install directory
    if [ -n "$INSTALL_DIR" ]; then
        BIN_DIR="$INSTALL_DIR"
    elif [ -w "/usr/local/bin" ]; then
        BIN_DIR="/usr/local/bin"
    elif [ -d "$HOME/.local/bin" ]; then
        BIN_DIR="$HOME/.local/bin"
    else
        mkdir -p "$HOME/.local/bin"
        BIN_DIR="$HOME/.local/bin"
    fi

    # Install
    if [ -w "$BIN_DIR" ]; then
        mv "$TMP_DIR/$BINARY" "$BIN_DIR/$BINARY"
        chmod +x "$BIN_DIR/$BINARY"
    else
        info "Installing to $BIN_DIR (requires sudo)..."
        sudo mv "$TMP_DIR/$BINARY" "$BIN_DIR/$BINARY"
        sudo chmod +x "$BIN_DIR/$BINARY"
    fi

    success "Successfully installed ${BINARY} ${VERSION} to ${BIN_DIR}/${BINARY}"

    # Check if in PATH
    if ! echo "$PATH" | grep -q "$BIN_DIR"; then
        warn "${BIN_DIR} is not in your PATH"
        echo "Add it with: export PATH=\"\$PATH:${BIN_DIR}\""
    fi

    # Verify installation
    if command -v "$BINARY" >/dev/null 2>&1; then
        echo ""
        "$BINARY" version
    fi
}

main "$@"
