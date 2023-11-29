"""
This modules implements a sqlite3.Connection subclass that supports
less suprising transaction semantics, including nested transactions
"""


import sqlite3
from typing import Literal

class SQLite3TransactionalConnection(sqlite3.Connection):
    """
    A sqlite3.Connection subclass that supports nested transactions
    """
    def __init__(self, *args, **kwargs) -> None:
        kwargs['isolation_level'] = None
        super().__init__(*args, **kwargs)
        self.__transaction_stack = []

    def __enter__(self):
        if len(self.__transaction_stack) == 0:
            assert not self.in_transaction
            self.execute('BEGIN TRANSACTION')
            self.__transaction_stack.append(None)
            return

        savepoint_name = f'savepoint-{len(self.__transaction_stack)}'
        self.execute(f"SAVEPOINT {savepoint_name}")
        self.__transaction_stack.append(savepoint_name)

    def __exit__(self, __type: type[BaseException] | None, __value: BaseException | None, __traceback) -> Literal[False]:
        assert self.in_transaction
        assert len(self.__transaction_stack) > 0
        savepoint_name = self.__transaction_stack[-1]

        # Happy Path
        if __type is None:
            if savepoint_name is None:
                self.execute("COMMIT TRANSACTION")
                assert not self.in_transaction
                self.__transaction_stack.pop()
                return False

            self.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            assert self.in_transaction
            self.__transaction_stack.pop()
            return False

        # Exited in Error
        if savepoint_name is None:
            self.execute("ROLLBACK TRANSACTION")
            assert not self.in_transaction
            self.__transaction_stack.pop()
            return False

        self.execute(f"ROLLBACK TRANSACTION TO SAVEPOINT {savepoint_name}")
        assert self.in_transaction
        self.__transaction_stack.pop()
        return False

def connect(*args, enable_wal = True, enable_fornkeys = True, **kwargs) -> sqlite3.Connection:
    """
    Create a new sqlite3.Connection with the specified arguments, but with
    sane transaction semantics, including nested transactions.

    If enable_wal is True, then the connection will be created with WAL mode
    enabled. This is the default.

    If enable_fornkeys is True, then the connection will be created with
    foreign keys enabled. This is the default.
    """
    result = sqlite3.connect(*args, factory=SQLite3TransactionalConnection, **kwargs)
    if enable_wal:
        result.execute("PRAGMA journal_mode=WAL")
        result.execute("PRAGMA synchronous=NORMAL")

    if enable_fornkeys:
        result.execute("PRAGMA foreign_keys=ON")

    return result
