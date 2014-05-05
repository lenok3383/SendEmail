class ConnectionRefused(Exception):
    pass


class NotAvailable(Exception):
    pass


class UnknownService(Exception):
    pass


class TerminationConnection(Exception):
    pass


class RequestedActionAborted(Exception):
    pass


class MySyntaxError(Exception):
    pass
