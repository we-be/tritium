#!/bin/sh
# Renders the formula for one release: the version, and the sha256 of each
# tarball the formula names, read from the tarballs `make dist` left in DIR.
#   packaging/homebrew/render.sh vX.Y.Z dist > dist/tritium.rb
set -eu
v=${1#v}
dir=$2
here=$(dirname "$0")

sum() {
	f="$dir/tritium-v$v-$1.tar.gz"
	[ -f "$f" ] || { echo "render.sh: no $f" >&2; return 1; }
	(sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") | cut -d' ' -f1
}
darwin_arm64=$(sum darwin-arm64)
darwin_amd64=$(sum darwin-amd64)
linux_arm64=$(sum linux-arm64)
linux_amd64=$(sum linux-amd64)

sed -e "s/0\.0\.0/$v/g" \
	-e "s/REPLACE_WITH_DARWIN_ARM64_SHA256/$darwin_arm64/" \
	-e "s/REPLACE_WITH_DARWIN_AMD64_SHA256/$darwin_amd64/" \
	-e "s/REPLACE_WITH_LINUX_ARM64_SHA256/$linux_arm64/" \
	-e "s/REPLACE_WITH_LINUX_AMD64_SHA256/$linux_amd64/" \
	"$here/tritium.rb"
