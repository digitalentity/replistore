#!/usr/bin/env bash
set -euo pipefail

PACKAGE_NAME="replistore"
VERSION="${VERSION:-$(./scripts/get_version.sh)}"
DEB_VERSION="${VERSION#v}"
ARCH="amd64"
MAINTAINER="Konstantin Sharlaimov <ksharlaimov@inavflight.com>"
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
mkdir -p "${STAGING_DIR}/var/lib/replistore"
chmod 750 "${STAGING_DIR}/var/lib/replistore"
mkdir -p "${STAGING_DIR}/mnt/replistore"
chmod 755 "${STAGING_DIR}/mnt/replistore"

mkdir -p "${STAGING_DIR}/etc/logrotate.d"

cp "${BUILD_DIR}/${PACKAGE_NAME}" "${STAGING_DIR}/usr/bin/"
cp config.yaml "${STAGING_DIR}/etc/replistore/config.yaml"

# Create logrotate config
cat <<EOF > "${STAGING_DIR}/etc/logrotate.d/replistore"
/var/log/replistore/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF

# Create conffiles
cat <<EOF > "${STAGING_DIR}/DEBIAN/conffiles"
/etc/replistore/config.yaml
/etc/logrotate.d/replistore
EOF

# Create systemd service file
cat <<EOF > "${STAGING_DIR}/lib/systemd/system/replistore.service"
[Unit]
Description=RepliStore Replicated Filesystem
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/replistore mount --config /etc/replistore/config.yaml
KillMode=process
Restart=on-failure
RestartSec=10
StandardOutput=append:/var/log/replistore/replistore.log
StandardError=append:/var/log/replistore/replistore.log
LogsDirectory=replistore

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
