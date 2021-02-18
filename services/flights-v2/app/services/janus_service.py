from gremlin_python import statics
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import WithOptions
import os

JANUS_HOST = (
    os.environ["JANUS_HOST"]
    if "JANUS_HOST" in os.environ
    else "ws://localhost:8182/gremlin"
)
g = traversal().withRemote(DriverRemoteConnection(JANUS_HOST, "g"))


def get_values(d):
    for k in d:
        if type(d[k]) is list:
            d[k] = d[k][0]
        else:
            d[k] = get_values(d[k])
    d.pop("object_type", None)
    return d


def merge_flight_data(flights):
    flight_data = {
        "flight_one_id": flights["flight1"]["id"],
        "source_airport_id": flights["flight1"]["source_airport_id"],
        "destination_airport_id": flights["flight1"]["destination_airport_id"],
        "time": flights["flight1"]["flight_duration"],
        "flight_one_time": flights["flight1"]["flight_time"],
        "cost": flights["flight1"]["cost"],
        "airlines": flights["flight1"]["airlines"],
    }

    if "flight2" in flights:
        flight_data["flight_two_id"] = flights["flight2"]["id"]
        flight_data["layover_one_airport_id"] = flights["flight2"]["source_airport_id"]
        flight_data["destination_airport_id"] = flights["flight2"][
            "destination_airport_id"
        ]
        flight_data["flight_time"] = (
            flights["flight1"]["flight_duration"]
            + flights["flight2"]["flight_duration"]
        )
        flight_data["time"] = (
            flights["flight1"]["flight_duration"]
            + flights["flight2"]["flight_time"]
            - flights["flight1"]["flight_time"]
            + flights["flight2"]["flight_duration"]
        )
        flight_data["cost"] = flights["flight1"]["cost"] + flights["flight2"]["cost"]
        flight_data["flight_two_time"] = flights["flight2"]["flight_time"]

    if "flight3" in flights:
        flight_data["flight_three_id"] = flights["flight3"]["id"]
        flight_data["layover_two_airport_id"] = flights["flight3"]["source_airport_id"]
        flight_data["destination_airport_id"] = flights["flight3"][
            "destination_airport_id"
        ]
        flight_data["flight_time"] = (
            flights["flight1"]["flight_duration"]
            + flights["flight2"]["flight_duration"]
            + flights["flight3"]["flight_duration"]
        )
        flight_data["time"] = (
            flights["flight1"]["flight_duration"]
            + flights["flight2"]["flight_time"]
            - flights["flight1"]["flight_time"]
            + flights["flight2"]["flight_duration"]
            + flights["flight3"]["flight_time"]
            - flights["flight2"]["flight_time"]
            + flights["flight3"]["flight_duration"]
        )
        flight_data["cost"] = (
            flights["flight1"]["cost"]
            + flights["flight2"]["cost"]
            + flights["flight3"]["cost"]
        )
        flight_data["flight_three_time"] = flights["flight3"]["flight_time"]

    return flight_data


def get_flight_info_from_janus(filter):
    try:
        res = (
            g.V()
            .has("object_type", "flight")
            .limit(30000)
            .values("airlines")
            .dedup()
            .toList()
        )
        return res
    except Exception as e:
        print(e)


def get_airport_from_janus(id, context):
    try:
        res = (
            g.V()
            .and_(
                __.hasLabel("airport"),
                __.has("id", id),
            )
            .valueMap()
            .next()
        )
        return get_values(res)
    except Exception as e:
        print(e)


def get_airports_from_janus(city, country, code, context):
    try:
        if city and country and code:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("city", city),
                    __.has("country", country),
                    __.has("iata_code", code),
                )
                .valueMap()
                .toList()
            )
        elif city and country:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("city", city),
                    __.has("country", country),
                )
                .valueMap()
                .toList()
            )
        elif city and code:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("city", city),
                    __.has("iata_code", code),
                )
                .valueMap()
                .toList()
            )
        elif country and code:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("country", country),
                    __.has("iata_code", code),
                )
                .valueMap()
                .toList()
            )
        elif city:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("city", city),
                )
                .valueMap()
                .toList()
            )
        elif country:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("country", country),
                )
                .valueMap()
                .toList()
            )
        elif code:
            res = (
                g.V()
                .and_(
                    __.hasLabel("airport"),
                    __.has("iata_code", code),
                )
                .valueMap()
                .toList()
            )
        else:
            res = []

        for i in range(0, len(res)):
            res[i] = get_values(res[i])
        return res
    except Exception as e:
        print(e)


def get_airports_list_from_janus(context):
    try:
        res = (
            g.V()
            .has("object_type", "airport")
            .valueMap("country", "city")
            .dedup()
            .toList()
        )

        for i in range(0, len(res)):
            res[i] = get_values(res[i])
        return res
    except Exception as e:
        print(e)


def get_direct_flights_from_janus(from_, to, context):
    try:
        res = (
            g.V()
            .hasLabel("airport")
            .has("id", from_)
            .bothE("departing")
            .otherV()
            .as_("flight1")
            .hasLabel("flight")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .has("id", to)
            .select("flight1")
            .valueMap()
            .toList()
        )

        for i in range(0, len(res)):
            res[i] = merge_flight_data({"flight1": get_values(res[i])})
        return res
    except Exception as e:
        print(e)


def get_onestop_flights_from_janus(from_, to, context):
    try:
        res = (
            g.V()
            .hasLabel("airport")
            .has("id", "9600276f-608f-4325-a037-f185848f2e28")
            .bothE("departing")
            .otherV()
            .as_("flight1")
            .hasLabel("flight")
            .values("flight_duration")
            .as_("fd")
            .select("flight1")
            .values("flight_time")
            .math("_ + fd + 60")
            .as_("flight1_time")
            .select("flight1")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .bothE("departing")
            .otherV()
            .as_("flight2")
            .hasLabel("flight")
            .values("flight_time")
            .math("_ - flight1_time")
            .is_(P.gte(0))
            .where("flight1", P.eq("flight2"))
            .by("airlines")
            .select("flight2")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .has("id", "ebc645cd-ea42-40dc-b940-69456b64d2dd")
            .select("flight1", "flight2")
            .by(__.valueMap())
            .limit(20)
            .toList()
        )

        for i in range(0, len(res)):
            res[i] = merge_flight_data(get_values(res[i]))
        return res
    except Exception as e:
        print(e)


def get_twostop_flights_from_janus(from_, to, context):
    try:
        res = (
            g.V()
            .hasLabel("airport")
            .has("id", "9600276f-608f-4325-a037-f185848f2e28")
            .bothE("departing")
            .otherV()
            .as_("flight1")
            .hasLabel("flight")
            .values("flight_duration")
            .as_("fd")
            .select("flight1")
            .values("flight_time")
            .math("_ + fd + 60")
            .as_("flight1_time")
            .select("flight1")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .bothE("departing")
            .otherV()
            .as_("flight2")
            .hasLabel("flight")
            .values("flight_time")
            .math("_ - flight1_time")
            .is_(P.gte(0))
            .where("flight1", P.eq("flight2"))
            .by("airlines")
            .select("flight2")
            .values("flight_duration")
            .as_("fd1")
            .select("flight2")
            .values("flight_time")
            .math("_ + fd1 + 60")
            .as_("flight2_time")
            .select("flight2")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .bothE("departing")
            .otherV()
            .as_("flight3")
            .hasLabel("flight")
            .values("flight_time")
            .math("_ - flight2_time")
            .is_(P.gte(0))
            .where("flight2", P.eq("flight3"))
            .by("airlines")
            .select("flight3")
            .bothE("arriving")
            .otherV()
            .hasLabel("airport")
            .has("id", "ebc645cd-ea42-40dc-b940-69456b64d2dd")
            .select("flight1", "flight2", "flight3")
            .by(__.valueMap())
            .limit(10)
            .toList()
        )

        for i in range(0, len(res)):
            res[i] = merge_flight_data(get_values(res[i]))
        return res
    except Exception as e:
        print(e)


def janus():
    try:
        # /api/v1/flights/info/airlines
        """
        airlines = g.V().hasLabel("flight").values("airlines").dedup().toList()
        print(airlines)
        """

        # /api/v1/flights/airports
        """
        airport = (
            g.V()
            .and_(
                __.hasLabel("airport"),
                __.has("country", "United States"),
                __.has("city", "New York"),
                __.has("iata_code", "JFK"),
            )
            .valueMap()
            .next()
        )
        print(airport)
        """

        # /api/v1/flights/airports/all
        """
        airports = g.V().hasLabel("airport").valueMap("country", "city").toList()
        print(airports)
        """

        # /api/v1/flights/airports/{id}
        """
        airport = (
            g.V()
            .and_(
                __.hasLabel("airport"),
                __.has("id", "9600276f-608f-4325-a037-f185848f2e28"),
            )
            .valueMap()
            .next()
        )
        print(airport)
        """

        # /api/v1/flights/direct/{from}/{to}
        """
        flights = (
            g.V()
            .and_(
                __.hasLabel("flight"),
                __.out("departing").has("id", "9600276f-608f-4325-a037-f185848f2e28"),
                __.out("arriving").has("id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"),
            )
            .valueMap()
            .toList()
        )
        print(flights)
        """

        # /api/v1/flights/onestop/{from}/{to}
        """
        flights = (
            g.V()
            .as_("flight1")
            .and_(
                __.hasLabel("flight"),
                __.out("departing").has("id", "9600276f-608f-4325-a037-f185848f2e28"),
            )
            .values("flight_duration")
            .as_("fd")
            .select("flight1")
            .values("flight_time")
            .math("_ + fd + 60")
            .as_("flight1_time")
            .select("flight1")
            .out("arriving")
            .in_("departing")
            .as_("flight2")
            .and_(
                __.hasLabel("flight"),
                __.out("arriving").has("id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"),
                __.values("flight_time").math("_ - flight1_time").is_(P.gte(0)),
            )
            .where("flight1", P.eq("flight2"))
            .by("airlines")
            .select("flight1", "flight2")
            .by(__.valueMap())
            .toList()
        )
        print(flights)
        """

        # /api/v1/flights/twostop/{from}/{to}
        """
        flights = (
            g.V()
            .as_("flight1")
            .and_(
                __.hasLabel("flight"),
                __.out("departing").has("id", "9600276f-608f-4325-a037-f185848f2e28"),
            )
            .values("flight_duration")
            .as_("fd")
            .select("flight1")
            .values("flight_time")
            .math("_ + fd + 60")
            .as_("flight1_time")
            .select("flight1")
            .out("arriving")
            .in_("departing")
            .as_("flight2")
            .and_(
                __.hasLabel("flight"),
                __.values("flight_time").math("_ - flight1_time").is_(P.gte(0)),
            )
            .where("flight1", P.eq("flight2"))
            .by("airlines")
            .values("flight_duration")
            .as_("fd1")
            .select("flight2")
            .values("flight_time")
            .math("_ + fd1 + 60")
            .as_("flight2_time")
            .select("flight2")
            .out("arriving")
            .in_("departing")
            .as_("flight3")
            .and_(
                __.hasLabel("flight"),
                __.out("arriving").has("id", "6275b58b-55a8-4c3f-93fa-372529ee0b2f"),
                __.values("flight_time").math("_ - flight2_time").is_(P.gte(0)),
            )
            .where("flight2", P.eq("flight3"))
            .by("airlines")
            .select("flight1", "flight2", "flight3")
            .by(__.valueMap())
            .toList()
        )
        print(flights)
        """

    except Exception as e:
        print(e)


def load_data():
    try:
        g.addV("airport").property("object_type", "airport").property(
            "id", "9600276f-608f-4325-a037-f185848f2e28"
        ).property("name", "Los Angeles International Airport").property(
            "is_hub", True
        ).property(
            "is_destination", True
        ).property(
            "type", "large_airport"
        ).property(
            "country", "United States"
        ).property(
            "city", "Los Angeles"
        ).property(
            "latitude", 33.94250107
        ).property(
            "longitude", -118.4079971
        ).property(
            "gps_code", "KLAX"
        ).property(
            "iata_code", "LAX"
        ).iterate()
        g.addV("airport").property("object_type", "airport").property(
            "id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"
        ).property("name", "John F. Kennedy International Airport").property(
            "is_hub", True
        ).property(
            "is_destination", True
        ).property(
            "type", "large_airport"
        ).property(
            "country", "United States"
        ).property(
            "city", "New York"
        ).property(
            "latitude", 40.63980103
        ).property(
            "longitude", -73.77890015
        ).property(
            "gps_code", "KJFK"
        ).property(
            "iata_code", "JFK"
        ).iterate()
        g.addV("airport").property("object_type", "airport").property(
            "id", "30a2b6e8-fcc2-4e59-a61d-3aff713b23b0"
        ).property("name", "Dallas Fort Worth International Airport").property(
            "is_hub", True
        ).property(
            "is_destination", False
        ).property(
            "type", "large_airport"
        ).property(
            "country", "United States"
        ).property(
            "city", "Dallas-Fort Worth"
        ).property(
            "latitude", 32.896801
        ).property(
            "longitude", -97.038002
        ).property(
            "gps_code", "KDFW"
        ).property(
            "iata_code", "DFW"
        ).iterate()
        g.addV("airport").property("object_type", "airport").property(
            "id", "6275b58b-55a8-4c3f-93fa-372529ee0b2f"
        ).property("name", "Lester B. Pearson International Airport").property(
            "is_hub", True
        ).property(
            "is_destination", True
        ).property(
            "type", "large_airport"
        ).property(
            "country", "Canada"
        ).property(
            "city", "Toronto"
        ).property(
            "latitude", 43.6772003174
        ).property(
            "longitude", -79.63059997559999
        ).property(
            "gps_code", "YYZ"
        ).property(
            "iata_code", "YYZ"
        ).iterate()
        g.addV("flight").property("object_type", "flight").property(
            "id", "e7c3d85d-c523-4634-93ef-a84f55aeb1e5"
        ).property(
            "source_airport_id", "9600276f-608f-4325-a037-f185848f2e28"
        ).property(
            "destination_airport_id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"
        ).property(
            "flight_time", 345
        ).property(
            "flight_duration", 324.384941546607
        ).property(
            "cost", 584.833849847819
        ).property(
            "airlines", "MilkyWay Airlines"
        ).iterate()
        g.addV("flight").property("object_type", "flight").property(
            "id", "fa3448ff-f157-4690-9180-0e06700ac909"
        ).property(
            "source_airport_id", "9600276f-608f-4325-a037-f185848f2e28"
        ).property(
            "destination_airport_id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"
        ).property(
            "flight_time", 945
        ).property(
            "flight_duration", 324.384941546607
        ).property(
            "cost", 483.08914733391794
        ).property(
            "airlines", "Phoenix Airlines"
        ).iterate()
        g.addV("flight").property("object_type", "flight").property(
            "id", "d76b8e14-3519-42eb-84ff-9a7406b43234"
        ).property(
            "source_airport_id", "9600276f-608f-4325-a037-f185848f2e28"
        ).property(
            "destination_airport_id", "30a2b6e8-fcc2-4e59-a61d-3aff713b23b0"
        ).property(
            "flight_time", 615
        ).property(
            "flight_duration", 176.90181296430302
        ).property(
            "cost", 345.5986288722164
        ).property(
            "airlines", "Spartan Airlines"
        ).iterate()
        g.addV("flight").property("object_type", "flight").property(
            "id", "cd29ac89-1b5e-4f8c-8e7c-d53404e6b092"
        ).property(
            "source_airport_id", "30a2b6e8-fcc2-4e59-a61d-3aff713b23b0"
        ).property(
            "destination_airport_id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"
        ).property(
            "flight_time", 1080
        ).property(
            "flight_duration", 195.54786163329285
        ).property(
            "cost", 368.5532617453382
        ).property(
            "airlines", "Spartan Airlines"
        ).iterate()
        g.addV("flight").property("object_type", "flight").property(
            "id", "0656f352-8903-406f-a55c-e83e70028302"
        ).property(
            "source_airport_id", "ebc645cd-ea42-40dc-b940-69456b64d2dd"
        ).property(
            "destination_airport_id", "6275b58b-55a8-4c3f-93fa-372529ee0b2f"
        ).property(
            "flight_time", 1350
        ).property(
            "flight_duration", 73.59971345361168
        ).property(
            "cost", 221.63444620384936
        ).property(
            "airlines", "Spartan Airlines"
        ).iterate()

        lax = g.V().has("iata_code", "LAX").next()
        jfk = g.V().has("iata_code", "JFK").next()
        dfw = g.V().has("iata_code", "DFW").next()
        yyz = g.V().has("iata_code", "YYZ").next()
        f1 = g.V().has("id", "e7c3d85d-c523-4634-93ef-a84f55aeb1e5").next()
        f2 = g.V().has("id", "fa3448ff-f157-4690-9180-0e06700ac909").next()
        f3 = g.V().has("id", "d76b8e14-3519-42eb-84ff-9a7406b43234").next()
        f4 = g.V().has("id", "cd29ac89-1b5e-4f8c-8e7c-d53404e6b092").next()
        f5 = g.V().has("id", "0656f352-8903-406f-a55c-e83e70028302").next()

        g.addE("departing").from_(f1).to(lax).iterate()
        g.addE("arriving").from_(f1).to(jfk).iterate()
        g.addE("departing").from_(f2).to(lax).iterate()
        g.addE("arriving").from_(f2).to(jfk).iterate()
        g.addE("departing").from_(f3).to(lax).iterate()
        g.addE("arriving").from_(f3).to(dfw).iterate()
        g.addE("departing").from_(f4).to(dfw).iterate()
        g.addE("arriving").from_(f4).to(jfk).iterate()
        g.addE("departing").from_(f5).to(jfk).iterate()
        g.addE("arriving").from_(f5).to(yyz).iterate()
    except Exception as e:
        print(e)
