#!/bin/sh

set -e

REPO="gigagrug/schema"
VERSION="${1:-latest}"
echo "Installing version: $VERSION"
TMP_DIR="$(mktemp -d)"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  linux*)   GOOS="linux" ;;
  darwin*)  GOOS="darwin" ;;
  msys*|mingw*|cygwin*) GOOS="windows" ;;
  *)        echo "❌ Unsupported OS: $OS" && exit 1 ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64)   GOARCH="amd64" ;;
  arm64|aarch64) GOARCH="arm64" ;;
  *)        echo "❌ Unsupported architecture: $ARCH" && exit 1 ;;
esac

EXT=""
if [ "$GOOS" = "windows" ]; then EXT=".exe"; fi
FILENAME="schema-${GOOS}-${GOARCH}${EXT}"
if [ "$VERSION" = "latest" ]; then 
	URL="https://github.com/${REPO}/releases/${VERSION}/download/${FILENAME}"
else
	URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}"
fi
echo "🔽 Downloading $FILENAME from $URL"
curl --fail -sSL "$URL" -o "$TMP_DIR/schema${EXT}" || {
  echo "❌ Failed to download schema binary. Version '$VERSION' may not exist."
  exit 1
}
chmod 700 "$TMP_DIR/schema${EXT}"

if [ "$GOOS" = "windows" ]; then
  INSTALL_DIR="$HOME/bin"
  INSTALL_PATH="${INSTALL_DIR}/schema.exe"
  echo "🚀 Installing to $INSTALL_PATH"
  mkdir -p "$INSTALL_DIR"
  mv "$TMP_DIR/schema${EXT}" "$INSTALL_PATH"
  echo "✅ Installed schema.exe to $INSTALL_PATH"
else
  INSTALL_DIR="/usr/local/bin"
  INSTALL_PATH="${INSTALL_DIR}/schema"
  echo "🚀 Installing to $INSTALL_PATH"
  sudo mv "$TMP_DIR/schema${EXT}" "$INSTALL_PATH"
  echo "✅ Installed schema to $INSTALL_PATH"
fi

rm -rf "$TMP_DIR"
