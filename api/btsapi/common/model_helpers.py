#@TODO: File documentation here
#
import json
from uuid import UUID
from sqlalchemy.ext.declarative import DeclarativeMeta
import  datetime

def dump_datetime(value):
    """Deserialize datetime object into string form for JSON processing."""
    if value is None:
        return None
    return [value.strftime("%Y-%m-%d"), value.strftime("%H:%M:%S")]
