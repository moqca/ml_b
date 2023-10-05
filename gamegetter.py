from typing import List
import datetime as dt
import requests
import re


def get_all_game_dates(start_year: int, end_year: int) -> List[List[dt.datetime]]:
    """
    Retrieves all game dates from the Baseball Reference website and returns a list of dates.

    Args:
    start_year (int): The starting year for retrieving game dates.
    end_year (int): The ending year (exclusive) for retrieving game dates.

    Returns:
    List[List[dt.datetime]]: A nested list of datetime objects representing game dates for each year.

    """
    all_dates: List[List[dt.datetime]] = []

    # Iterate over the years
    for date in range(start_year, end_year):
        # Construct the URL for the year's schedule
        url = f'https://www.baseball-reference.com/leagues/MLB/{date}-schedule.shtml'

        # Send a GET request to the URL
        resp = requests.get(url, verify=False)

        # Extract the game dates using regular expressions
        days = re.findall("<h3>(.*{year})</h3>".format(year=date), resp.text)

        # Convert the extracted dates to datetime objects
        dates = [dt.datetime.strptime(d, "%A, %B %d, %Y") for d in days]

        # Append the dates to the list
        all_dates.append(dates)

    # Get the date string for indexing purposes
    date_str = all_dates[5][0].strftime('%Y-%m-%d')

    return all_dates

