#!/bin/sh
# Seed script: creates Alice and Bob, opens an account for each, funds each with $1000.
# Requires the API to be running at BASE_URL.
set -e

BASE_URL="${BASE_URL:-http://localhost:8000}"

register_and_fund() {
    name="$1"
    email="$2"
    password="$3"
    amount="$4"

    echo "--- $name ---"

    # Register
    register_resp=$(curl -sf -X POST "$BASE_URL/v1/auth/register" \
        -H "Content-Type: application/json" \
        -d "{\"email\": \"$email\", \"password\": \"$password\"}")
    echo "Registered: $(echo "$register_resp" | grep -o '"email":"[^"]*"')"

    # Login
    login_resp=$(curl -sf -X POST "$BASE_URL/v1/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"email\": \"$email\", \"password\": \"$password\"}")
    token=$(echo "$login_resp" | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

    # Open account
    account_resp=$(curl -sf -X POST "$BASE_URL/v1/accounts" \
        -H "Authorization: Bearer $token")
    account_id=$(echo "$account_resp" | sed 's/.*"account_id":"\([^"]*\)".*/\1/')
    echo "Account:    $account_id"

    # Fund account
    idempotency_key="seed-$name-$amount"
    fund_resp=$(curl -sf -X POST "$BASE_URL/v1/dev/seed" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $idempotency_key" \
        -d "{\"account_id\": \"$account_id\", \"amount\": \"$amount\"}")
    balance=$(echo "$fund_resp" | sed 's/.*"balance":"\([^"]*\)".*/\1/')
    echo "Balance:    $balance"
    echo ""
}

register_and_fund "alice" "alice@minibank.dev" "password123" "1000"
register_and_fund "bob"   "bob@minibank.dev"   "password123" "1000"

echo "Done. Alice and Bob are ready."
