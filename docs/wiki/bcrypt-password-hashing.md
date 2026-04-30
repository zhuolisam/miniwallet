# bcrypt Password Hashing

Used in [[alembic-qna|auth_service.py]] to hash and verify user passwords. bcrypt is a C library — it operates on raw bytes, not Python strings.

---

## `str.encode()` and `bytes.decode()`

Python strings (`str`) and byte sequences (`bytes`) are distinct types. bcrypt requires `bytes`.

```python
"hello".encode()        # str → bytes:  b"hello"
b"hello".decode()       # bytes → str:  "hello"
```

`encode()` with no argument defaults to UTF-8. You'll see this pattern everywhere bcrypt is used:

```python
# hashing — password is str, bcrypt wants bytes
hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
#                               ^^^^^^^^                   ^^^^^^^^
#                         str → bytes            bytes → str (for DB storage)

# verifying — both inputs must be bytes
bcrypt.checkpw(password.encode(), stored_hash.encode())
#                      ^^^^^^^^             ^^^^^^^^
#                str → bytes      str from DB → bytes
```

---

## Salts

You don't configure a salt — `bcrypt.gensalt()` generates a **random salt automatically** on every call.

The salt is **embedded inside the hash string** itself:

```
$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/o19qxg7lG
      ^^^^^^^^^^^^^^^^^^^^^^^^^^
      cost factor + salt (stored here)
```

At verification, bcrypt extracts the salt from the stored hash and re-hashes the input with it. No separate salt column needed.

**Why this matters:** every user gets a unique random salt. Two users with the same password produce completely different hashes, defeating precomputed rainbow table attacks.

---

## Cost Factor

`bcrypt.gensalt(rounds=12)` — 12 is the default. Higher = slower to compute = harder to brute-force, but also slower for every login. 12 is the current industry standard for web apps.

```python
bcrypt.gensalt()         # rounds=12 (default)
bcrypt.gensalt(rounds=14) # slower, higher security
```

---

## In MiniBank

```python
# app/services/auth_service.py

# Registration
hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
# → stored in users.hashed_password (VARCHAR)

# Login
bcrypt.checkpw(password.encode(), user.hashed_password.encode())
# → True if password matches, False otherwise
```

bcrypt is used directly (not via passlib) — passlib is unmaintained as of 2024.
