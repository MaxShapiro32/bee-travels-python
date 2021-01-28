from app.services import data_handler
from app.errors import tag_not_found, illegal_date
from app.jaeger import Jaeger
from flask import jsonify, request, Blueprint
from datetime import datetime
from pybreaker import CircuitBreaker

flights_blueprint = Blueprint("flights_blueprint", __name__)

breaker = CircuitBreaker(fail_max=5, reset_timeout=30)

context = Jaeger()


def string_to_array(string):
    return string.split(",")


def get_query_param(key, query_data, func):
    if key in query_data.keys():
        if query_data[key] == "NaN":
            return None
        return func(query_data[key])
    return None


@flights_blueprint.route("/", methods=["GET"])
def flights():
    """
    /**
    * GET /api/v1/flights
    * @description Example route
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("flights", request)
    try:
        data = breaker.call(data_handler.get_data, context)
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/load", methods=["GET"])
def load_flights():
    """
    /**
    * GET /api/v1/flights/load
    * @description Example route
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("load_flights", request)
    try:
        data = breaker.call(data_handler.load_data, context)
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/info/<filter>", methods=["GET"])
def get_flight_info(filter):
    """
    /**
    * GET /api/v1/flights/info/{filter}
    * @description Get info about flights
    * @pathParam filter info to look up
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("flight info", request)
    try:
        data = breaker.call(data_handler.get_filter_list, filter)
        status_code = 200
    except tag_not_found.TagNotFounException as e:
        data = {"error": e.args[0]}
        status_code = 400
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/airports", methods=["GET"])
def get_airports():
    """
    /**
    * GET /api/v1/flights/airports
    * @description Get all airports
    * @queryParam {string} [country] - Country of the rental company using slug casing (ex. united-states)
    * @queryParam {string} [city] - City of the rental company using slug casing (ex. new-york)
    * @queryParam {string} [code] - 3 Letter iata code for the airport
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("airports", request)
    try:
        query_data = request.args
        data = breaker.call(
            data_handler.get_airports,
            get_query_param("city", query_data, str),
            get_query_param("country", query_data, str),
            get_query_param("code", query_data, str),
            context,
        )
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/airports/all", methods=["GET"])
def get_all_airports():
    """
    /**
    * GET /api/v1/flights/airports/all
    * @description Get all cities and countries we have flights from
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("allAirports", request)
    try:
        data = breaker.call(data_handler.get_airports_list, context)
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/airports/<id>", methods=["GET"])
def get_airport(id):
    """
    /**
    * GET /api/v1/flights/airports/{id}
    * @description Get airport details for a specific airport
    * @pathParam {string} id - ID of a airport
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("airport", request)
    try:
        data = breaker.call(data_handler.get_airport, id, context)
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/direct/<from_>/<to>", methods=["GET"])
def get_direct(from_, to):
    """
    /**
    * GET /api/v1/flights/direct/{from}/{to}
    * @description Get all direct flight to destination
    * @pathParam from source airport id
    * @pathParam to destination airport id
    * @queryParam {string} dateFrom - Date From
    * @queryParam {string} dateTo - Date To
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("direct", request)
    try:
        query_data = request.args
        try:
            date_from = get_query_param("dateFrom", query_data, parse_date)
            date_to = get_query_param("dateTo", query_data, parse_date)
        except Exception:
            raise illegal_date.IllegalDateException("needs a date")
        data = breaker.call(
            data_handler.get_direct_flights,
            from_,
            to,
            {"date_from": date_from, "date_to": date_to},
            context,
        )
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/onestop/<from_>/<to>", methods=["GET"])
def get_onestop(from_, to):
    """
    /**
    * GET /api/v1/flights/onestop/{from}/{to}
    * @description Get all one stop flight to destination
    * @pathParam from source airport id
    * @pathParam to destination airport id
    * @queryParam {string} dateFrom - Date From
    * @queryParam {string} dateTo - Date To
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("onestop", request)
    try:
        query_data = request.args
        try:
            date_from = get_query_param("dateFrom", query_data, parse_date)
            date_to = get_query_param("dateTo", query_data, parse_date)
        except Exception:
            raise illegal_date.IllegalDateException("needs a date")
        data = breaker.call(
            data_handler.get_onestop_flights,
            from_,
            to,
            {"date_from": date_from, "date_to": date_to},
            context,
        )
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


@flights_blueprint.route("/twostop/<from_>/<to>", methods=["GET"])
def get_twostop(from_, to):
    """
    /**
    * GET /api/v1/flights/twostop/{from}/{to}
    * @description Get all two stop flight to destination
    * @pathParam from source airport id
    * @pathParam to destination airport id0
    * @queryParam {string} dateFrom - Date From
    * @queryParam {string} dateTo - Date To
    * @response 200 - OK
    * @response 400 - Error
    */
    """

    context.start("twostop", request)
    try:
        query_data = request.args
        try:
            date_from = get_query_param("dateFrom", query_data, parse_date)
            date_to = get_query_param("dateTo", query_data, parse_date)
        except Exception:
            raise illegal_date.IllegalDateException("needs a date")
        data = breaker.call(
            data_handler.get_twostop_flights,
            from_,
            to,
            {"date_from": date_from, "date_to": date_to},
            context,
        )
        status_code = 200
    except Exception as e:
        data = {"error": e.args[0]}
        status_code = 400
    finally:
        context.stop(status_code)
        return jsonify(data), status_code


def parse_date(date):
    return datetime.strptime(date, "%Y-%m-%d")
