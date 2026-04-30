"""Domain exception classes for MiniBank business logic.

Each subclass declares its own HTTP status, error code, and message as class attributes.
A single generic handler in main.py reads these — no per-exception registration needed.
"""


class MiniBankError(Exception):
    """Base for all domain errors. Subclasses override the three class attributes below."""
    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"
    message: str = "An internal error occurred"


class EmailAlreadyExistsError(MiniBankError):
    status_code = 409
    error_code = "EMAIL_ALREADY_EXISTS"
    message = "Email already registered"


class InvalidCredentialsError(MiniBankError):
    status_code = 401
    error_code = "INVALID_CREDENTIALS"
    message = "Invalid email or password"


class InvalidRefreshTokenError(MiniBankError):
    status_code = 401
    error_code = "INVALID_REFRESH_TOKEN"
    message = "Refresh token invalid or expired"


class UnauthorizedError(MiniBankError):
    status_code = 401
    error_code = "UNAUTHORIZED"
    message = "Invalid or expired token"


class AccountAlreadyExistsError(MiniBankError):
    status_code = 409
    error_code = "ACCOUNT_ALREADY_EXISTS"
    message = "User already has an account"


class AccountNotFoundError(MiniBankError):
    status_code = 404
    error_code = "NOT_FOUND"
    message = "Account not found"


class UserNotFoundError(MiniBankError):
    status_code = 404
    error_code = "NOT_FOUND"
    message = "User not found"


class TransferNotFoundError(MiniBankError):
    status_code = 404
    error_code = "NOT_FOUND"
    message = "Transfer not found"


class InsufficientBalanceError(MiniBankError):
    status_code = 422
    error_code = "INSUFFICIENT_BALANCE"
    message = "Insufficient balance"


class SameAccountError(MiniBankError):
    status_code = 422
    error_code = "SAME_ACCOUNT"
    message = "Cannot transfer to yourself"


class IdempotencyConflictError(MiniBankError):
    status_code = 409
    error_code = "IDEMPOTENCY_CONFLICT"
    message = "Same idempotency key used with different request parameters"


class IdempotencyKeyConsumedError(MiniBankError):
    status_code = 409
    error_code = "IDEMPOTENCY_KEY_CONSUMED"
    message = "This idempotency key was used for a previous failed request. Generate a new key to retry."


class MissingIdempotencyKeyError(MiniBankError):
    status_code = 400
    error_code = "MISSING_IDEMPOTENCY_KEY"
    message = "Idempotency-Key header required"


class ForbiddenError(MiniBankError):
    status_code = 403
    error_code = "FORBIDDEN"
    message = "Dev endpoints not available in this environment"
