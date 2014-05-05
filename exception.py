class ConnectionRefusedException(Exception):
    pass


class NotAvailableException(Exception):
    pass


class UnknownServiceException(Exception):
    pass


class TerminationConnectionException(Exception):
    pass


class RequestedActionAbortedException(Exception):
    pass


class SyntaxErrorException(Exception):
    pass
