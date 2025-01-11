class NonConflictingCommitException(Exception):
    """
    Exception raised when a non-conflicting commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)
