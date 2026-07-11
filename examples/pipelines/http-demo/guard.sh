#!/bin/sh
set -eu

: "${BOARKSHOP_EVENT_FILE:?BOARKSHOP_EVENT_FILE is required}"

if grep -Fq '"source":"http"' "$BOARKSHOP_EVENT_FILE" &&
   grep -Fq '"method":"POST"' "$BOARKSHOP_EVENT_FILE" &&
   grep -Fq '"path":"/demo"' "$BOARKSHOP_EVENT_FILE"; then
    exit 0
fi

exit 1

