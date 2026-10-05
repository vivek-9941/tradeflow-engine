#!/bin/bash
set -e

# Requires ghz to be installed (https://ghz.sh/)
# Usage: ./run_ghz.sh [target_address]

TARGET=${1:-"localhost:50051"}
PROTO_FILE="../proto/matching.proto"

echo "=== Running gRPC Load Test with ghz ==="

# We use the -c (concurrency) and -n (total requests) flags.
# The payload is provided via a JSON string.
ghz --insecure \
    --proto="${PROTO_FILE}" \
    --call="trade.matching.MatchingEngine/SubmitOrder" \
    -d '{"order_id": "GHZ_123", "user_id": "U1", "symbol": "RELIANCE", "side": "BUY", "order_type": "LIMIT", "price_paise": 10000, "quantity": 10}' \
    -c 50 \
    -n 100000 \
    "${TARGET}" | tee ghz_output.txt

echo "Load test complete. Results saved to ghz_output.txt"
