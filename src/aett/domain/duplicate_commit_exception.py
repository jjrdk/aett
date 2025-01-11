class DuplicateCommitException(Exception):
    """
    Exception raised when a duplicate commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)
