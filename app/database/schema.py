from datetime import datetime, timedelta, timezone
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from core.config import settings


class URLItem(BaseModel):
    """Request model for creating a new shortened URL.

    Args:
        original_url (str): The original URL to be shortened.
        slug (Optional[str]): Optional custom slug for the shortened URL.
        expiry_date (Optional[datetime]): Optional expiry date for the shortened URL.
        start_date (Optional[datetime]): Optional start date for the shortened URL.
    """

    original_url: str = Field(
        ...,
        description="Original URL to be shortened",
        example="https://example.com",
    )
    slug: Optional[str] = Field(
        None,
        description="Optional custom slug for the shortened URL",
        example="custom-slug",
    )
    expiry_date: Optional[datetime] = Field(
        None,
        description=(
            "Optional expiry date for the shortened URL in ISO 8601 format"
            " (offset-naive)"
        ),
        example="2023-12-31T23:59:59",
    )
    start_date: Optional[datetime] = Field(
        None,
        description=(
            "Optional start date for the shortened URL in ISO 8601 format"
            " (offset-naive)"
        ),
        example="2023-01-01T00:00:00",
    )

    @model_validator(mode="after")
    def validate_dates(cls, values):
        """Validate and set default values for start and expiry dates."""

        now = datetime.now(timezone.utc)
        start_date = values.start_date
        if start_date is None:
            start_date = now

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        else:
            start_date = start_date.astimezone(timezone.utc)

        expiry_date = values.expiry_date
        if expiry_date is None:
            expiry_date = now + timedelta(days=settings.DEFAULT_EXPIRY_DURATION)

        if expiry_date.tzinfo is None:
            expiry_date = expiry_date.replace(tzinfo=timezone.utc)
        else:
            expiry_date = expiry_date.astimezone(timezone.utc)

        if expiry_date < now:
            raise ValueError("Expiry date must be in the future.")

        if expiry_date < start_date:
            raise ValueError("Expiry date must be after start date.")

        values.start_date = start_date.replace(tzinfo=None)
        values.expiry_date = expiry_date.replace(tzinfo=None)

        return values


class CreateURL(URLItem):
    """Request model for creating a new shortened URL.

    Args:
        original_url (str): The original URL to be shortened.
        slug (Optional[str]): Optional custom slug for the shortened URL.
        expiry_date (Optional[datetime]): Optional expiry date for the shortened URL.
        start_date (Optional[datetime]): Optional start date for the shortened URL.
    """


class CreateURLBatch(BaseModel):
    """Request model for creating a batch of new shortened URLs.

    Args:
        urls (list[URLItem]): List of URLs to be shortened.
    """

    urls: list[URLItem] = Field(
        ...,
        description="List of URLs to be shortened",
        example=[
            {
                "original_url": "https://example.com",
                "slug": "custom-slug",
                "expiry_date": "2023-12-31T23:59:59",
                "start_date": "2023-01-01T00:00:00",
            },
            {
                "original_url": "https://another-example.com",
                "slug": None,
                "expiry_date": None,
                "start_date": None,
            },
        ],
    )
