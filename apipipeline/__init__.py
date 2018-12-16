import os

# Initialize Sentry before importing the rest of the app.
sentry_sdk = None

if "SENTRY_DSN" in os.environ:
    import sentry_sdk

    sentry_sdk.init(
        dsn=os.environ["SENTRY_DSN"],
    )
