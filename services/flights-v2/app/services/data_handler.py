from app.services import janus_service
from app.errors import tag_not_found, database_not_found, illegal_date
import os
from datetime import datetime, timedelta

filter_types = ["airlines", "type"]

# Cities with these words in the city name are lower case
lowercase_exceptions = ["es", "de", "au"]


def capitalize(text):
    if not text:
        return text

    text = list(
        map(
            lambda s: s if s in lowercase_exceptions else s.capitalize(),
            text.lower().split("-"),
        )
    )

    # The city of Port-au-Prince keeps "-" between the words of the city
    return "-".join(text) if lowercase_exceptions[2] in text else " ".join(text)


def upper(text):
    return text.upper() if text else text


def get_filter_list(filter):
    if filter not in filter_types:
        raise tag_not_found.TagNotFounException(filter)
    if filter == "type":
        return ["non-stop", "one-stop", "two-stop"]
    if os.environ["DATABASE"] == "janus":
        return janus_service.get_flight_info_from_janus(filter)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])


def get_airports(city, country, code, context):
    if os.environ["DATABASE"] == "janus":
        data = janus_service.get_airports_from_janus(
            capitalize(city), capitalize(country), upper(code), context
        )
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return data


def get_airports_list(context):
    if os.environ["DATABASE"] == "janus":
        data = janus_service.get_airports_list_from_janus(context)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return data


def get_airport(id, context):
    if os.environ["DATABASE"] == "janus":
        data = janus_service.get_airport_from_janus(id, context)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return data


def get_direct_flights(from_, to, filters, context):
    if filters["date_to"] - filters["date_from"] < timedelta(0):
        raise illegal_date.IllegalDateException(
            "from date can not be greater than to date"
        )

    if os.environ["DATABASE"] == "janus":
        flights = janus_service.get_direct_flights_from_janus(from_, to, context)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return update_cost(flights, filters["date_from"])


def get_onestop_flights(from_, to, filters, context):
    if filters["date_to"] - filters["date_from"] < timedelta(0):
        raise illegal_date.IllegalDateException(
            "from date can not be greater than to date"
        )

    if os.environ["DATABASE"] == "janus":
        flights = janus_service.get_onestop_flights_from_janus(from_, to, context)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return update_cost(flights, filters["date_from"], 0.75)


def get_twostop_flights(from_, to, filters, context):
    if filters["date_to"] - filters["date_from"] < timedelta(0):
        raise illegal_date.IllegalDateException(
            "from date can not be greater than to date"
        )

    if os.environ["DATABASE"] == "janus":
        flights = janus_service.get_twostop_flights_from_janus(from_, to, context)
    else:
        raise database_not_found.DatabaseNotFoundException(os.environ["DATABASE"])

    return update_cost(flights, filters["date_from"], 0.5)


def readiness_check():
    if os.environ["DATABASE"] == "janus":
        return janus_service.janus_readiness_check()
    else:
        return False


def update_cost(data, date, scale=1):
    multiplier = date_multiplier(date)

    for flight in data:
        flight["cost"] = flight["cost"] * multiplier * scale

    return data


def date_multiplier(date_from):
    date_now = datetime.now()
    num_days = (date_from - date_now).days
    if num_days < 0:
        raise illegal_date.IllegalDateException(date_from)
    elif num_days < 2:
        return 2.25
    elif num_days < 7:
        return 1.75
    elif num_days < 14:
        return 1.5
    elif num_days < 21:
        return 1.2
    elif num_days < 45:
        return 1
    elif num_days < 90:
        return 0.8
    else:
        raise illegal_date.IllegalDateException(date_from)


def get_data(context):
    context.start("getData")
    context.stop()
    janus_service.janus()


def load_data(context):
    context.start("loadData")
    context.stop()
    janus_service.load_data()
