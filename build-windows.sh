#!/bin/bash
# Build script for BV-BRC CLI tools - Windows installer

set -e

# Use GO env var if set, then the local dev path if present, then PATH fallback
GO="${GO:-/home/olson/P3/go-1.25.6/go/bin/go}"
command -v "$GO" &>/dev/null || GO=go
VERSION="${VERSION:-$(sh "$(dirname "$0")/scripts/version.sh")}"
# Stamp the version into the binaries; it is what the User-Agent reports.
LDFLAGS_VERSION="-X github.com/BV-BRC/BV-BRC-Go-SDK/version.Version=${VERSION}"
OUTPUT_DIR="dist"

cd "$(dirname "$0")"
SDK_DIR="$(pwd)"

# Archive directory names (each zip expands into a versioned directory).
DIST_AMD64="bvbrc-cli-${VERSION}-windows-amd64"
DIST_ARM64="bvbrc-cli-${VERSION}-windows-arm64"

# Get list of all commands
COMMANDS=$(ls -d cmd/p3-*/ | xargs -n1 basename)
CMD_COUNT=$(echo $COMMANDS | wc -w)

echo "Building BV-BRC CLI tools v${VERSION} for Windows"
echo "Commands to build: $CMD_COUNT"

# Build for Windows
build_windows() {
    local ARCH=$1
    local ARCH_NAME=$2

    echo ""
    echo "Building for Windows $ARCH_NAME ($ARCH)..."

    local BIN_DIR="$OUTPUT_DIR/bvbrc-cli-${VERSION}-windows-$ARCH"
    mkdir -p "$BIN_DIR"

    for cmd in $COMMANDS; do
        echo "  $cmd.exe"
        GOOS=windows GOARCH=$ARCH CGO_ENABLED=0 $GO build -buildvcs=false -ldflags="-s -w $LDFLAGS_VERSION" -o "$BIN_DIR/$cmd.exe" "./cmd/$cmd"
    done
}

# Clean and build
rm -rf "$OUTPUT_DIR/windows-"*
rm -rf "$OUTPUT_DIR/bvbrc-cli-"*"-windows-"*

build_windows "amd64" "x64"
build_windows "arm64" "ARM64"

# Create zip archives
echo ""
echo "Creating distribution archives..."

# Archives are created after installers, README, and LICENSE are added below.

# Create batch file installer
cat > "$OUTPUT_DIR/$DIST_AMD64/install.bat" << 'EOF'
@echo off
REM BV-BRC CLI Tools Installer for Windows
REM Run this as Administrator

echo BV-BRC CLI Tools Installer
echo ==========================
echo.

REM Check for admin rights
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo This script requires Administrator privileges.
    echo Please right-click and select "Run as administrator"
    pause
    exit /b 1
)

REM Create installation directory
set INSTALL_DIR=C:\Program Files\BVBRC
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"

REM Copy executables
echo Installing to %INSTALL_DIR%...
copy /Y *.exe "%INSTALL_DIR%\" >nul

REM Add to PATH
echo Adding to system PATH...
setx /M PATH "%PATH%;%INSTALL_DIR%" >nul 2>&1

echo.
echo Installation complete!
echo.
echo Please restart your command prompt or PowerShell to use the tools.
echo.
echo To get started:
echo   1. Open a new Command Prompt or PowerShell
echo   2. Run: p3-login your-username
echo   3. Run: p3-ls to list your workspace
echo.
pause
EOF

cp "$OUTPUT_DIR/$DIST_AMD64/install.bat" "$OUTPUT_DIR/$DIST_ARM64/install.bat"

# Create PowerShell installer
cat > "$OUTPUT_DIR/$DIST_AMD64/install.ps1" << 'EOF'
# BV-BRC CLI Tools Installer for Windows (PowerShell)
# Run as Administrator: powershell -ExecutionPolicy Bypass -File install.ps1

$ErrorActionPreference = "Stop"

Write-Host "BV-BRC CLI Tools Installer" -ForegroundColor Cyan
Write-Host "==========================" -ForegroundColor Cyan
Write-Host ""

# Check for admin rights
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "This script requires Administrator privileges." -ForegroundColor Red
    Write-Host "Please run PowerShell as Administrator and try again." -ForegroundColor Red
    exit 1
}

# Installation directory
$InstallDir = "C:\Program Files\BVBRC"

# Create directory if it doesn't exist
if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}

# Copy executables
Write-Host "Installing to $InstallDir..."
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Copy-Item "$ScriptDir\*.exe" -Destination $InstallDir -Force

# Add to PATH
$currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
if ($currentPath -notlike "*$InstallDir*") {
    Write-Host "Adding to system PATH..."
    [Environment]::SetEnvironmentVariable("Path", "$currentPath;$InstallDir", "Machine")
}

Write-Host ""
Write-Host "Installation complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Please restart your PowerShell or Command Prompt to use the tools."
Write-Host ""
Write-Host "To get started:" -ForegroundColor Yellow
Write-Host "  1. Open a new PowerShell or Command Prompt"
Write-Host "  2. Run: p3-login your-username"
Write-Host "  3. Run: p3-ls to list your workspace"
Write-Host ""
EOF

cp "$OUTPUT_DIR/$DIST_AMD64/install.ps1" "$OUTPUT_DIR/$DIST_ARM64/install.ps1"

# Create README + LICENSE for Windows (README via the shared generator)
bash "$SDK_DIR/scripts/make-readme.sh" "$VERSION" "windows-amd64" > "$OUTPUT_DIR/$DIST_AMD64/README.txt"
bash "$SDK_DIR/scripts/make-readme.sh" "$VERSION" "windows-arm64" > "$OUTPUT_DIR/$DIST_ARM64/README.txt"
cp "$SDK_DIR/LICENSE" "$OUTPUT_DIR/$DIST_AMD64/LICENSE"
cp "$SDK_DIR/LICENSE" "$OUTPUT_DIR/$DIST_ARM64/LICENSE"

# Create the distribution archives (each expands into a versioned directory
# containing the .exe tools, installers, README, and LICENSE).
cd "$OUTPUT_DIR"
if command -v zip &> /dev/null; then
    rm -f "${DIST_AMD64}.zip" "${DIST_ARM64}.zip"
    zip -rq "${DIST_AMD64}.zip" "$DIST_AMD64"
    echo "  Created ${DIST_AMD64}.zip"
    zip -rq "${DIST_ARM64}.zip" "$DIST_ARM64"
    echo "  Created ${DIST_ARM64}.zip"
else
    tar -czf "${DIST_AMD64}.tar.gz" "$DIST_AMD64"
    echo "  Created ${DIST_AMD64}.tar.gz (zip not available)"
    tar -czf "${DIST_ARM64}.tar.gz" "$DIST_ARM64"
    echo "  Created ${DIST_ARM64}.tar.gz (zip not available)"
fi
cd ..

# Create NSIS installer script (for building on Windows with NSIS)
mkdir -p "$OUTPUT_DIR/nsis"
# Parse VERSION into major.minor.build for NSIS
IFS='.' read -r VER_MAJOR VER_MINOR VER_BUILD <<< "${VERSION:-1.0.0}"
VER_MAJOR=${VER_MAJOR:-1}; VER_MINOR=${VER_MINOR:-0}; VER_BUILD=${VER_BUILD:-0}
OUTFILE="bvbrc-cli-${VERSION}-windows-amd64-setup.exe"

cat > "$OUTPUT_DIR/nsis/bvbrc-cli.nsi" << NSIS_EOF
; BV-BRC CLI Tools NSIS Installer Script
; Compile with: makensis bvbrc-cli.nsi
; No external plugins required.

!define APPNAME "BV-BRC CLI Tools"
!define COMPANYNAME "BV-BRC"
!define APPVERSION "${VERSION}"
!define INSTALLSIZE 350000
!define REGKEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\BV-BRC CLI Tools"
!define PATHKEY "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"

RequestExecutionLevel admin

InstallDir "\$PROGRAMFILES64\BVBRC"

Name "\${APPNAME} \${APPVERSION}"
Icon "\${NSISDIR}\Contrib\Graphics\Icons\modern-install.ico"
OutFile "${OUTFILE}"

!include "MUI2.nsh"
!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "license.txt"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"

Section "Install"
    SetOutPath \$INSTDIR

    ; Copy all executables
    File "*.exe"

    ; Create uninstaller
    WriteUninstaller "\$INSTDIR\uninstall.exe"

    ; Add install dir to system PATH (append via registry — no plugin needed)
    ReadRegStr \$0 HKLM "\${PATHKEY}" "Path"
    WriteRegExpandStr HKLM "\${PATHKEY}" "Path" "\$0;\$INSTDIR"
    SendMessage \${HWND_BROADCAST} \${WM_WININICHANGE} 0 "STR:Environment" /TIMEOUT=5000

    ; Add uninstall information to Add/Remove Programs
    WriteRegStr HKLM "\${REGKEY}" "DisplayName" "\${APPNAME} \${APPVERSION}"
    WriteRegStr HKLM "\${REGKEY}" "UninstallString" '"\$INSTDIR\uninstall.exe"'
    WriteRegStr HKLM "\${REGKEY}" "InstallLocation" "\$INSTDIR"
    WriteRegStr HKLM "\${REGKEY}" "Publisher" "\${COMPANYNAME}"
    WriteRegStr HKLM "\${REGKEY}" "DisplayVersion" "\${APPVERSION}"
    WriteRegDWORD HKLM "\${REGKEY}" "EstimatedSize" \${INSTALLSIZE}
SectionEnd

Section "Uninstall"
    ; Remove files
    Delete "\$INSTDIR\*.exe"
    Delete "\$INSTDIR\uninstall.exe"
    RMDir "\$INSTDIR"

    ; Remove uninstall registry key
    DeleteRegKey HKLM "\${REGKEY}"
SectionEnd
NSIS_EOF

# Export the expected output filename for the workflow
echo "NSIS_OUTFILE=${OUTFILE}" >> "${GITHUB_ENV:-/dev/null}"

# License file shown by the NSIS installer — use the project LICENSE.
cp "$SDK_DIR/LICENSE" "$OUTPUT_DIR/nsis/license.txt"

# Copy executables to Inno Setup directory
echo ""
echo "Preparing Inno Setup installer..."
mkdir -p "$OUTPUT_DIR/innosetup"
cp "$OUTPUT_DIR/$DIST_AMD64/"*.exe "$OUTPUT_DIR/innosetup/" 2>/dev/null || true
echo "  Copied executables to innosetup/"

echo ""
echo "========================================"
echo "Windows build complete!"
echo "========================================"
echo ""
echo "Distribution files:"
ls -lh "$OUTPUT_DIR"/*windows* 2>/dev/null || true
echo ""
echo "Installation options for end users:"
echo ""
echo "  1. Extract zip and run install.bat as Administrator"
echo ""
echo "  2. Extract zip and run in PowerShell as Administrator:"
echo "     powershell -ExecutionPolicy Bypass -File install.ps1"
echo ""
echo "  3. Manual: Copy .exe files to a directory and add to PATH"
echo ""
echo "To create a graphical installer:"
echo ""
echo "  Using Inno Setup (recommended):"
echo "    1. Install Inno Setup from https://jrsoftware.org/isinfo.php"
echo "    2. Copy dist/innosetup to Windows"
echo "    3. Run: iscc bvbrc-cli.iss"
echo "    Output: bvbrc-cli-${VERSION}-windows-x64-setup.exe"
echo ""
echo "  Using NSIS:"
echo "    1. Install NSIS from https://nsis.sourceforge.io/"
echo "    2. Copy ${DIST_AMD64}/*.exe to nsis/"
echo "    3. Run: makensis nsis/bvbrc-cli.nsi"
