#!/bin/sh
printf 'hello from %s, via launcher: %s\n' "$(uname -s)" "${VIA_LAUNCHER:-no}"
