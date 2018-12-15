import os

# Initialize Sentry before importing the rest of the app.
sentry_sdk = None

if "SENTRY_DSN" in os.environ:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.celery import CeleryIntegration

    sentry_sdk.init(
        dsn=os.environ["SENTRY_DSN"],
    )
