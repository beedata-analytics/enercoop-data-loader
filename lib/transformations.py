from datetime import datetime, timedelta


def date_converter(dt, format, last_second=False, str_format=None):
    """ Util to convert a string into datetime object or another string with format changes
    
    :param dt: datetime string
    :param format: format to parse dt
    :param last_second: if we need to transform a day date into last moment of that day
    :param str_format: if we need to transform datetime into string again
    """
    ts = datetime.strptime(dt, format)
    if last_second:
        ts = ts + timedelta(days=1) - timedelta(seconds=1)
        
    if str_format:
        ts = ts.strftime(str_format)
    
    return ts

def str2bool(value):
    """ Convert string into boolean """
    return value.lower() in ['yes', 'true', '1', 'y']


