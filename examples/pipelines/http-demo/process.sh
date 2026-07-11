#!/bin/sh
set -eu

: "${BOARKSHOP_EVENT_FILE:?BOARKSHOP_EVENT_FILE is required}"
: "${BOARKSHOP_DATA_DIR:?BOARKSHOP_DATA_DIR is required}"
: "${BOARKSHOP_EVENT_ID:?BOARKSHOP_EVENT_ID is required}"

destination="$BOARKSHOP_DATA_DIR/last-event.json"
temporary="$destination.tmp.$$"
trap 'rm -f "$temporary"' EXIT HUP INT TERM

cp "$BOARKSHOP_EVENT_FILE" "$temporary"
mv "$temporary" "$destination"
trap - EXIT HUP INT TERM

printf 'processed event %s\n' "$BOARKSHOP_EVENT_ID"

