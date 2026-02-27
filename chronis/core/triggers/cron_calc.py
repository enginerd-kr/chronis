"""
Cron field parser and next-fire-time calculator.

Based on APScheduler 3.x CronTrigger (MIT License).
Simplified to support only the field types used by Chronis:
year, month, day, week, day_of_week, hour, minute.
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import datetime, timedelta, tzinfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]

MIN_VALUES = {
    "year": 1970,
    "month": 1,
    "day": 1,
    "week": 1,
    "day_of_week": 0,
    "hour": 0,
    "minute": 0,
}
MAX_VALUES = {
    "year": 9999,
    "month": 12,
    "day": 31,
    "week": 53,
    "day_of_week": 6,
    "hour": 23,
    "minute": 59,
}
DEFAULT_VALUES = {
    "year": "*",
    "month": 1,
    "day": 1,
    "week": "*",
    "day_of_week": "*",
    "hour": 0,
    "minute": 0,
}

FIELD_NAMES = ("year", "month", "day", "week", "day_of_week", "hour", "minute")
SEPARATOR = re.compile(r" *, *")


def _asint(text: str | int | None) -> int | None:
    if text is not None:
        return int(text)
    return None


def _datetime_ceil(dateval: datetime) -> datetime:
    """Round datetime up to the next whole minute."""
    if dateval.second > 0 or dateval.microsecond > 0:
        return dateval.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return dateval.replace(second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


class AllExpression:
    value_re = re.compile(r"\*(?:/(?P<step>\d+))?$")

    def __init__(self, step: str | int | None = None):
        self.step = _asint(step)
        if self.step == 0:
            raise ValueError("Increment must be higher than 0")

    def get_next_value(self, date: datetime, field: BaseField) -> int | None:
        start = field.get_value(date)
        minval = field.get_min(date)
        maxval = field.get_max(date)
        start = max(start, minval)

        if not self.step:
            nxt = start
        else:
            distance_to_next = (self.step - (start - minval)) % self.step
            nxt = start + distance_to_next

        if nxt <= maxval:
            return nxt
        return None


class RangeExpression(AllExpression):
    value_re = re.compile(r"(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$")

    def __init__(
        self, first: str | int, last: str | int | None = None, step: str | int | None = None
    ):
        super().__init__(step)
        first = _asint(first)  # type: ignore[assignment]
        last = _asint(last)  # type: ignore[assignment]
        if last is None and step is None:
            last = first
        if last is not None and first > last:  # type: ignore[operator]
            raise ValueError("The minimum value in a range must not be higher than the maximum")
        self.first: int = first  # type: ignore[assignment]
        self.last: int | None = last  # type: ignore[assignment]

    def get_next_value(self, date: datetime, field: BaseField) -> int | None:
        startval = field.get_value(date)
        minval = field.get_min(date)
        maxval = field.get_max(date)

        minval = max(minval, self.first)
        maxval = min(maxval, self.last) if self.last is not None else maxval
        nextval = max(minval, startval)

        if self.step:
            distance_to_next = (self.step - (nextval - minval)) % self.step
            nextval += distance_to_next

        return nextval if nextval <= maxval else None


class MonthRangeExpression(RangeExpression):
    value_re = re.compile(r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE)

    def __init__(self, first: str, last: str | None = None):
        try:
            first_num = MONTHS.index(first.lower()) + 1
        except ValueError:
            raise ValueError(f'Invalid month name "{first}"') from None
        last_num = None
        if last:
            try:
                last_num = MONTHS.index(last.lower()) + 1
            except ValueError:
                raise ValueError(f'Invalid month name "{last}"') from None
        super().__init__(first_num, last_num)


class WeekdayRangeExpression(RangeExpression):
    value_re = re.compile(r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE)

    def __init__(self, first: str, last: str | None = None):
        try:
            first_num = WEEKDAYS.index(first.lower())
        except ValueError:
            raise ValueError(f'Invalid weekday name "{first}"') from None
        last_num = None
        if last:
            try:
                last_num = WEEKDAYS.index(last.lower())
            except ValueError:
                raise ValueError(f'Invalid weekday name "{last}"') from None
        super().__init__(first_num, last_num)


class WeekdayPositionExpression(AllExpression):
    options = ["1st", "2nd", "3rd", "4th", "5th", "last"]
    value_re = re.compile(
        r"(?P<option_name>{}) +(?P<weekday_name>(?:\d+|\w+))".format("|".join(options)),
        re.IGNORECASE,
    )

    def __init__(self, option_name: str, weekday_name: str):
        super().__init__(None)
        try:
            self.option_num = self.options.index(option_name.lower())
        except ValueError:
            raise ValueError(f'Invalid weekday position "{option_name}"') from None
        try:
            self.weekday = WEEKDAYS.index(weekday_name.lower())
        except ValueError:
            raise ValueError(f'Invalid weekday name "{weekday_name}"') from None

    def get_next_value(self, date: datetime, field: BaseField) -> int | None:
        first_day_wday, last_day = monthrange(date.year, date.month)
        first_hit_day = self.weekday - first_day_wday + 1
        if first_hit_day <= 0:
            first_hit_day += 7
        if self.option_num < 5:
            target_day = first_hit_day + self.option_num * 7
        else:
            target_day = first_hit_day + ((last_day - first_hit_day) // 7) * 7
        if target_day <= last_day and target_day >= date.day:
            return target_day
        return None


class LastDayOfMonthExpression(AllExpression):
    value_re = re.compile(r"last", re.IGNORECASE)

    def __init__(self):
        super().__init__(None)

    def get_next_value(self, date: datetime, field: BaseField) -> int | None:
        return monthrange(date.year, date.month)[1]


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


class BaseField:
    REAL = True
    COMPILERS: list[type[AllExpression]] = [AllExpression, RangeExpression]

    def __init__(self, name: str, exprs: str | int, is_default: bool = False):
        self.name = name
        self.is_default = is_default
        self.expressions: list[AllExpression] = []
        self._compile_expressions(exprs)

    def get_min(self, dateval: datetime) -> int:
        return MIN_VALUES[self.name]

    def get_max(self, dateval: datetime) -> int:
        return MAX_VALUES[self.name]

    def get_value(self, dateval: datetime) -> int:
        return getattr(dateval, self.name)

    def get_next_value(self, dateval: datetime) -> int | None:
        smallest = None
        for expr in self.expressions:
            value = expr.get_next_value(dateval, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value
        return smallest

    def _compile_expressions(self, exprs: str | int) -> None:
        for expr in SEPARATOR.split(str(exprs).strip()):
            self._compile_expression(expr)

    def _compile_expression(self, expr: str) -> None:
        for compiler in self.COMPILERS:
            match = compiler.value_re.match(expr)
            if match:
                compiled_expr = compiler(**match.groupdict())
                self.expressions.append(compiled_expr)
                return
        raise ValueError(f'Unrecognized expression "{expr}" for field "{self.name}"')


class WeekField(BaseField):
    REAL = False

    def get_value(self, dateval: datetime) -> int:
        return dateval.isocalendar()[1]


class DayOfMonthField(BaseField):
    COMPILERS = BaseField.COMPILERS + [WeekdayPositionExpression, LastDayOfMonthExpression]

    def get_max(self, dateval: datetime) -> int:
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField):
    REAL = False
    COMPILERS = BaseField.COMPILERS + [WeekdayRangeExpression]

    def get_value(self, dateval: datetime) -> int:
        return dateval.weekday()


class MonthField(BaseField):
    COMPILERS = BaseField.COMPILERS + [MonthRangeExpression]


FIELDS_MAP: dict[str, type[BaseField]] = {
    "year": BaseField,
    "month": MonthField,
    "week": WeekField,
    "day": DayOfMonthField,
    "day_of_week": DayOfWeekField,
    "hour": BaseField,
    "minute": BaseField,
}


# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------


def _build_fields(
    *,
    year: str | int | None = None,
    month: str | int | None = None,
    day: str | int | None = None,
    week: str | int | None = None,
    day_of_week: str | int | None = None,
    hour: str | int | None = None,
    minute: str | int | None = None,
) -> list[BaseField]:
    values: dict[str, str | int] = {}
    local_args = {
        "year": year,
        "month": month,
        "day": day,
        "week": week,
        "day_of_week": day_of_week,
        "hour": hour,
        "minute": minute,
    }
    for key, value in local_args.items():
        if value is not None:
            values[key] = value

    fields: list[BaseField] = []
    assign_defaults = False
    for field_name in FIELD_NAMES:
        if field_name in values:
            exprs = values.pop(field_name)
            is_default = False
            assign_defaults = not values
        elif assign_defaults:
            exprs = DEFAULT_VALUES[field_name]  # type: ignore[assignment]
            is_default = True
        else:
            exprs = "*"
            is_default = True

        field_class = FIELDS_MAP[field_name]
        fields.append(field_class(field_name, exprs, is_default))
    return fields


def _increment_field_value(
    dateval: datetime,
    fieldnum: int,
    fields: list[BaseField],
    tz: tzinfo | None,
) -> tuple[datetime, int]:
    """Increment designated field and reset less significant fields to minimums."""
    values: dict[str, int] = {}
    i = 0
    while i < len(fields):
        field = fields[i]
        if not field.REAL:
            if i == fieldnum:
                fieldnum -= 1
                i -= 1
            else:
                i += 1
            continue

        if i < fieldnum:
            values[field.name] = field.get_value(dateval)
            i += 1
        elif i > fieldnum:
            values[field.name] = field.get_min(dateval)
            i += 1
        else:
            value = field.get_value(dateval)
            maxval = field.get_max(dateval)
            if value == maxval:
                fieldnum -= 1
                i -= 1
            else:
                values[field.name] = value + 1
                i += 1

    difference = datetime(**values) - dateval.replace(tzinfo=None)  # type: ignore[arg-type]
    dateval = datetime.fromtimestamp(dateval.timestamp() + difference.total_seconds(), tz)
    return dateval, fieldnum


def _set_field_value(
    dateval: datetime,
    fieldnum: int,
    new_value: int,
    fields: list[BaseField],
) -> datetime:
    """Set designated field to new_value and reset less significant fields to minimums."""
    values: dict[str, int] = {}
    for i, field in enumerate(fields):
        if field.REAL:
            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
            else:
                values[field.name] = new_value
    return datetime(**values, tzinfo=dateval.tzinfo, fold=dateval.fold)


def calculate_next_cron_time(
    current: datetime,
    *,
    year: str | int | None = None,
    month: str | int | None = None,
    day: str | int | None = None,
    week: str | int | None = None,
    day_of_week: str | int | None = None,
    hour: str | int | None = None,
    minute: str | int | None = None,
) -> datetime | None:
    """Calculate next cron fire time after *current*.

    Args:
        current: Timezone-aware datetime to start searching from.
        year, month, day, week, day_of_week, hour, minute:
            Cron field expressions (same syntax as APScheduler 3.x).

    Returns:
        Next fire time as timezone-aware datetime, or None.
    """
    fields = _build_fields(
        year=year,
        month=month,
        day=day,
        week=week,
        day_of_week=day_of_week,
        hour=hour,
        minute=minute,
    )
    tz = current.tzinfo

    # Handle folded datetimes (DST fall-back)
    if current.fold == 1:
        current = datetime.fromisoformat(current.isoformat()) + timedelta(microseconds=1)

    next_date = _datetime_ceil(current).astimezone(tz)

    fieldnum = 0
    while 0 <= fieldnum < len(fields):
        field = fields[fieldnum]
        curr_value = field.get_value(next_date)
        next_value = field.get_next_value(next_date)

        if next_value is None:
            next_date, fieldnum = _increment_field_value(next_date, fieldnum - 1, fields, tz)
        elif next_value > curr_value:
            if field.REAL:
                next_date = _set_field_value(next_date, fieldnum, next_value, fields)
                fieldnum += 1
            else:
                next_date, fieldnum = _increment_field_value(next_date, fieldnum, fields, tz)
        else:
            fieldnum += 1

    if fieldnum >= 0:
        return next_date
    return None
