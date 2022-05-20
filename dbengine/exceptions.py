class BranchError(Exception):
    def __init__(self, message="Branch error occurred"):
        super().__init__(message)


class ProhibitedActionInBranch(BranchError):
    def __init__(self, message="This action is prohibited in this branch"):
        super().__init__(message)
