class ExpiredEntryError(Exception):
    """Raised when an URL is expired."""

    pass


class LinkNotActiveYetError(Exception):
    """Raised when an URL is not yet active."""

    pass


class SlugAlreadyExistsError(Exception):
    """Raised when a slug already exists."""

    pass