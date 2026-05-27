#!/bin/bash
# Run this once on the always-on Mac to install the iMessage sender as a permanent service.
# It will start on boot and restart automatically if it crashes.
#
# Usage: bash setup_mac_service.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$SCRIPT_DIR/mac_imessage_listener.py"
PLIST="$HOME/Library/LaunchAgents/com.legacyhometeam.imessage.plist"
LOG_DIR="$HOME/Library/Logs/LegacyHomeTeam"

echo "==> Setting up Legacy Home Team iMessage sender..."

# 1. Check dependencies
echo "Checking Python..."
python3 -c "import flask, requests" 2>/dev/null || {
    echo "Installing flask and requests..."
    pip3 install flask requests --quiet
}
echo "  flask + requests OK"

# 2. Test AppleScript / Messages access
echo "Testing iMessage access..."
osascript -e 'tell application "Messages" to get every account' > /dev/null 2>&1 || {
    echo ""
    echo "  WARNING: Messages.app may not be open or iMessage may not be signed in."
    echo "  Open Messages.app and sign in before texts will send."
    echo ""
}
echo "  iMessage OK"

# 3. Create log directory
mkdir -p "$LOG_DIR"

# 4. Write launchd plist
cat > "$PLIST" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.legacyhometeam.imessage</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>$SCRIPT</string>
        <string>--poll</string>
        <string>--interval</string>
        <string>60</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>$LOG_DIR/imessage_sender.log</string>

    <key>StandardErrorPath</key>
    <string>$LOG_DIR/imessage_sender.log</string>

    <key>ThrottleInterval</key>
    <integer>30</integer>
</dict>
</plist>
PLIST

echo "  Launchd plist written to $PLIST"

# 5. Load the service
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load "$PLIST"

echo ""
echo "==> Done. iMessage sender is running and will start on every boot."
echo "    Logs: $LOG_DIR/imessage_sender.log"
echo ""
echo "==> To check status:  launchctl list | grep legacyhometeam"
echo "    To view logs:      tail -f $LOG_DIR/imessage_sender.log"
echo "    To stop:           launchctl unload $PLIST"
echo "    To restart:        launchctl unload $PLIST && launchctl load $PLIST"
