#!/usr/bin/env bash
set -euo pipefail

PACKAGE_NAME="replistore"
VERSION="${VERSION:-$(./scripts/get_version.sh)}"
DEB_VERSION="${VERSION#v}"
ARCH="amd64"
MAINTAINER="Konstantin Sharlaimov <konstantin@digitalentity.net>"
DESCRIPTION="FUSE-based replicated filesystem over SMB"

BUILD_DIR="build"
STAGING_DIR="/tmp/replistore-deb-build"

echo "Building Debian package for ${PACKAGE_NAME} ${VERSION}..."

if [ ! -f "${BUILD_DIR}/${PACKAGE_NAME}" ]; then
    echo "Error: Binary not found. Run 'make build' first."
    exit 1
fi

rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}/DEBIAN"
mkdir -p "${STAGING_DIR}/usr/bin"
mkdir -p "${STAGING_DIR}/etc/replistore"
mkdir -p "${STAGING_DIR}/lib/systemd/system"

cp "${BUILD_DIR}/${PACKAGE_NAME}" "${STAGING_DIR}/usr/bin/"
cp config.yaml "${STAGING_DIR}/etc/replistore/config.yaml.example"

# Create systemd service file
cat <<EOF > "${STAGING_DIR}/lib/systemd/system/replistore.service"
[Unit]
Description=RepliStore Replicated Filesystem
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/replistore -config /etc/replistore/config.yaml
KillMode=process
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Create control file
cat <<EOF > "${STAGING_DIR}/DEBIAN/control"
Package: ${PACKAGE_NAME}
Version: ${DEB_VERSION}
Section: utils
Priority: optional
Architecture: ${ARCH}
Maintainer: ${MAINTAINER}
Description: ${DESCRIPTION}
Depends: fuse3
EOF

dpkg-deb --build "${STAGING_DIR}" "${BUILD_DIR}/${PACKAGE_NAME}_${DEB_VERSION}_${ARCH}.deb"
rm -rf "${STAGING_DIR}"

echo "Debian package created successfully: ${BUILD_DIR}/${PACKAGE_NAME}_${DEB_VERSION}_${ARCH}.deb"
