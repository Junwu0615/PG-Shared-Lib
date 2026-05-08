# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, timezone

TZ_UTC_0 = timezone(timedelta(hours=0))
TZ_UTC_8 = timezone(timedelta(hours=8))

INFO_BLANK = ' ' * 40
WARNING_BLANK = ' ' * 43
ERROR_BLANK = ' ' * 41

SHORT_FORMAT = '%Y-%m-%d'
LONG_FORMAT = '%Y-%m-%d %H:%M:%S'
LONG_T_FORMAT = '%Y-%m-%dT%H:%M:%S'