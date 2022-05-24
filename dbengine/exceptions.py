class BranchError(Exception):
    def __init__(self, message="Branch error occurred"):
        super().__init__(message)


class ProhibitedActionInBranch(BranchError):
    def __init__(self, action: str, branch_name: str):
        message = f"{action} is prohibited in {branch_name}"
        super().__init__(message)


class IncorrectBranchType(BranchError):
    def __init__(self, action: str, branch_name: str):
        message = f"You can't {action} this branch with {branch_name}"
        super().__init__(message)
