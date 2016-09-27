# Exception that catches index page (par ex. index.html)
class IndexPageException(EnvironmentError):
    pass


# Output type exception (if output type != 's', 'f' or 'd')
class OutputTypeException(Exception):
    pass