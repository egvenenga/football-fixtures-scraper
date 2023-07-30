import json
import requests
import pandas as pd
import logging
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.fotmob.com/api"


def get_league_fixtures(session, id, season):
    """
    Function to fetch league details and matches from the API

    Args:
    session : requests.Session instance
    id : id of the league
    season : the season for which data is needed

    Returns:
    details and matches data if successful, None otherwise
    """
    league_url = f"{BASE_URL}/leagues"
    params = {"id": id, "season": season}

    try:
        response = session.get(league_url, params=params)
        response.raise_for_status()

        data = response.json()

        return data["details"], data["matches"]["allMatches"]

    except RequestException as e:
        logger.error(f"An error occurred while trying to get {league_url}: {e}")
        return None, None


def create_details_dict(details):
    """
    Function to create a details dictionary from league details data

    Args:
    details : league details data

    Returns:
    details dictionary
    """
    return {
        "country": details["country"],
        "name": details["name"],
        "selectedSeason": details["selectedSeason"],
        "type": details["type"],
    }


def create_matches_dict(matches):
    """
    Function to create a list of match dictionaries from matches data

    Args:
    matches : matches data

    Returns:
    list of match dictionaries
    """
    return [
        {
            "away_team": match["away"]["name"],
            "home_team": match["home"]["name"],
            "round": match["round"],
            "cancelled": match["status"]["cancelled"],
            "time": match["status"]["utcTime"],
        }
        for match in matches
    ]


def main():
    leagues = [
        {"league_id": 87, "league_name": "laliga"},
        {"league_id": 47, "league_name": "premier-league"},
        {"league_id": 54, "league_name": "bundesliga"},
        {"league_id": 55, "league_name": "serie-a"},
        {"league_id": 53, "league_name": "ligue-1"},
        {"league_id": 57, "league_name": "eredivisie"},
    ]

    season = "2023/2024"

    final_df = pd.DataFrame()

    with requests.Session() as s:
        for league in leagues:
            league_id = league["league_id"]
            league_name = league["league_name"]

            details, matches = get_league_fixtures(s, league_id, season)

            if details and matches:
                details_dict = create_details_dict(details)
                matches_dict = create_matches_dict(matches)

                df = pd.DataFrame(matches_dict)

                # Add the details from details_dict to the DataFrame
                df = df.assign(**details_dict)

                final_df = pd.concat([final_df, df], ignore_index=True)

        final_df["country"] = final_df["country"].map(
            {
                "ESP": "Spain",
                "ENG": "England",
                "GER": "Germany",
                "ITA": "Italy",
                "FRA": "France",
                "NED": "Netherlands",
            }
        )

        return final_df


if __name__ == "__main__":
    df = main()

    df.to_csv("./matches.csv", index=False)
