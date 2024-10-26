class ConflictingCommitException(Exception):
    """
    Exception raised when a conflicting commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)
