from sqlalchemy import create_engine, text

def loadsql(filename: str) -> str:
    with open(f'sql\\{filename}.sql') as f:
        return f.read()

QUERY_FLIGHT_BY_ID = loadsql('flight_by_id')

QUERY_FLIGHT_BY_DATE = loadsql('flight_by_date')

QUERY_DELAYED_FLIGHTS_BY_AIRPORT = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY FROM flights JOIN airlines ON flights.airline = airlines.id WHERE flights.DEPARTURE_DELAY > 20 AND flights.ORIGIN_AIRPORT = :AIRPORT"

# Define the database URL
DATABASE_URL = "sqlite:///data/flights.sqlite3"

# Create the engine
engine = create_engine(DATABASE_URL)


def execute_query(query, params):
    """
    Execute an SQL query with the params provided in a dictionary,
    and returns a list of records (dictionary-like objects).
    If an exception was raised, print the error, and return an empty list.
    """
    try:
        with engine.connect() as conn:
            # your code here
            result = conn.execute(text(query), params)
            conn.commit()
        if result.returns_rows:
            # Convert rows to dictionary-like objects
            # Using result._tuple_getter is an internal method, but effective.
            # A more robust way might be to get column names first
            return result.fetchall()
        else:
            # For INSERT, UPDATE, DELETE, etc., you might commit and return nothing or rowcount
            conn.commit() # Important for DML operations
            return [] # Or return result.rowcount if that's desired
    except Exception as e:
        print("Query error:", e)
        return []
def get_delayed_flights_by_airport(airport: str):
    """
    Searches for flight details using flight ID.
    If the flight was found, returns a list with a single record.
    """
    params = {'AIRPORT': airport}
    return execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRPORT, params)

def get_flights_by_date(day: int, month: int, year: int):
    """
    Searches for flight details using flight ID.
    If the flight was found, returns a list with a single record.
    """
    params = {'DAY': day,'MONTH': month,'YEAR': year}
    return execute_query(QUERY_FLIGHT_BY_DATE, params)

def get_flight_by_id(flight_id):
    """
    Searches for flight details using flight ID.
    If the flight was found, returns a list with a single record.
    """
    params = {'id': flight_id}
    return execute_query(QUERY_FLIGHT_BY_ID, params)

    