from datetime import datetime, timedelta
import pytz


def parse_datetime(datums: str, dt: datetime):
    lv_month_numbers = {
        "janvāris": 1,
        "februāris": 2,
        "marts": 3,
        "aprīlis": 4,
        "maijs": 5,
        "jūnijs": 6,
        "jūlijs": 7,
        "augusts": 8,
        "septembris": 9,
        "oktobris": 10,
        "novembris": 11,
        "decembris": 12,
    }

    str_parts = datums.split(",")

    if len(str_parts) == 3:
        day, month_str = str_parts[0].split(". ")
        month = lv_month_numbers[month_str]
        year = str_parts[1]
        hour, min = str_parts[2].replace(" ", "").split(":")

    else:  # len is 2
        hour, min = str_parts[1].replace(" ", "").split(":")

        if str_parts[0].lower() == "vakar":
            yesterday = dt - timedelta(days=1)
            day = yesterday.day
            month = yesterday.month
            year = yesterday.year

        elif str_parts[0].lower() == "šodien":
            day = dt.day
            month = dt.month
            year = dt.year

        else:
            # Date without year -> current year
            day, month_str = str_parts[0].split(". ")
            month = lv_month_numbers[month_str]
            year = dt.year

    date = datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(min),
    )
    # Add timezone information to the datetime object
    timezone = pytz.timezone("Europe/Riga")
    # Preserve the existing time
    localized_date = timezone.localize(date, is_dst=None)

    return localized_date

if __name__ == "__main__":
    # Example usage

    dt = datetime.now()
    datums = "1. februāris, 08:30"
    parsed_date = parse_datetime(datums, dt)
    # Figures out the year and winter timezone
    print(parsed_date)

    datums = "Šodien, 08:30"
    parsed_date = parse_datetime(datums, dt)
    # Figures out its today and what timezone
    print(parsed_date)