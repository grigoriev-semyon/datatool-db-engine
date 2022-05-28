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


class TableError(Exception):
    def __init__(self, message="Table error occurred"):
        super().__init__(message)


class TableDoesntExists(TableError):
    def __init__(self, table_id: int, branch_name: str):
        super().__init__(message=f"Table with id {table_id} doesn't exists in {branch_name} branch")


class TableDeleted(TableError):
    def __init__(self, table_id: int, branch_name: str):
        super().__init__(message=f"Table {table_id} was deleted in {branch_name} branch")


class ColumnError(Exception):
    def __init__(self, message="Column error occurred"):
        super().__init__(message)


class ColumnDoesntExists(ColumnError):
    def __init__(self, column_id: int, branch_name: str):
        super().__init__(message=f"Column with id {column_id} doesn't exists in {branch_name} branch")


class ColumnDeleted(ColumnError):
    def __init__(self, column_id: int, branch_name: str):
        super().__init__(message=f"Table {column_id} was deleted in {branch_name} branch")
