def refill_tokens(
    tokens: int, capacity: int, last_refill_s: int, now_s: int, refill_rate_per_sec: int
):
    # Calculate elapsed time in seconds
    elapsed_s = now_s - last_refill_s

    # Calculate new tokens earned in this elapsed time
    new_tokens = elapsed_s * refill_rate_per_sec

    # Cap at capacity
    tokens = min(capacity, tokens + new_tokens)

    return tokens
