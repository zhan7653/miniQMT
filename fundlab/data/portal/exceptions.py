from fundlab.common.exceptions import FundLabError


class DataPortalError(FundLabError):
    """Base error for DataPortal failures."""


class InvalidPriceMode(DataPortalError, TypeError):
    pass


class AdjustedDataUnavailable(DataPortalError):
    pass
