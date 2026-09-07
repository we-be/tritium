# Not submitted to any tap. To install locally:
#   brew install --formula packaging/homebrew/tritium.rb
# or, from a tap that vendors this file, `brew install we-be/tritium/tritium`.
#
# Installs the five binaries (`tritium`, `tritium-cli`, `tritium-monitor`,
# `tritium-msg`, `tritium-load`) from the tarballs `make dist` builds and the
# release workflow publishes — no compiler needed on the machine installing
# this, matching a zero-dependency project that ships as one static binary.
class Tritium < Formula
  desc "RAM-only, zero-dependency key-value store that speaks the Redis protocol"
  homepage "https://github.com/we-be/tritium"
  version "0.11.1"
  license "GPL-3.0-only"

  # On a release: bump `version` above to the new tag (without the "v"), then
  # for each of the four URLs below, download the asset and run
  #   shasum -a 256 tritium-vX.Y.Z-OS-ARCH.tar.gz
  # and paste the result over the matching REPLACE_WITH_..._SHA256 placeholder.
  # (linux-arm, the GOARM=6 build for a Pi Zero, isn't packaged here — not a
  # Homebrew target.)
  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/we-be/tritium/releases/download/v#{version}/tritium-v#{version}-darwin-arm64.tar.gz"
      sha256 "REPLACE_WITH_DARWIN_ARM64_SHA256"
    else
      url "https://github.com/we-be/tritium/releases/download/v#{version}/tritium-v#{version}-darwin-amd64.tar.gz"
      sha256 "REPLACE_WITH_DARWIN_AMD64_SHA256"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/we-be/tritium/releases/download/v#{version}/tritium-v#{version}-linux-arm64.tar.gz"
      sha256 "REPLACE_WITH_LINUX_ARM64_SHA256"
    else
      url "https://github.com/we-be/tritium/releases/download/v#{version}/tritium-v#{version}-linux-amd64.tar.gz"
      sha256 "REPLACE_WITH_LINUX_AMD64_SHA256"
    end
  end

  def install
    bin.install "tritium", "tritium-cli", "tritium-monitor", "tritium-msg", "tritium-load"
  end

  test do
    # No live node needed: run with no subcommand and tritium-cli prints its
    # usage and exits 2, which is enough to prove the binary is the real one.
    output = shell_output("#{bin}/tritium-cli 2>&1", 2)
    assert_match "usage: tritium-cli", output
  end
end
