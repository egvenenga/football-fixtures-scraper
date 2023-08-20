import requests
import logging
import pandas as pd
from typing import Tuple, List, Dict, Union, Optional
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.fotmob.com/api"


def get_league_fixtures(
    session: requests.Session, league_id: int, season: str
) -> Tuple[Optional[Dict], Optional[List[Dict]]]:
    """
    Fetch league details and matches from the Fotmob API.

    Args:
        session: The session instance to use for the request.
        league_id: ID of the league to fetch.
        season: The target season in the format 'YYYY/YYYY'.

    Returns:
        League details and matches data if successful, (None, None) otherwise.
    """
    league_url = f"{BASE_URL}/leagues"
    params = {"id": league_id, "season": season}

    try:
        response = session.get(league_url, params=params)
        response.raise_for_status()

        data = response.json()

        return data["details"], data["matches"]["allMatches"]
    except RequestException as e:
        logger.error(
            f"An error occurred while fetching data for league ID {league_id}: {e}"
        )
        return None, None


def extract_details(details: Dict) -> Dict[str, Union[str, int]]:
    """
    Extract key details from the league details data.

    Args:
        details: Raw league details data.

    Returns:
        Extracted details.
    """
    return {
        "country": details["country"],
        "name": details["name"],
        "selectedSeason": details["selectedSeason"],
        "type": details["type"],
    }


def extract_matches(matches: List[Dict]) -> List[Dict[str, Union[str, int, bool]]]:
    """
    Extract key details from matches data and structure it in a list of dictionaries.

    Args:
        matches: Raw matches data.

    Returns:
        A list of dictionaries with key match details.
    """
    return [
        {
            "away_team": match.get("away", {}).get("name"),
            "home_team": match.get("home", {}).get("name"),
            "round": match.get("round"),
            "cancelled": match.get("status", {}).get("cancelled"),
            "finished": match.get("status", {}).get("finished"),
            "date": match.get("status", {}).get("utcTime"),
            "result": match.get("status", {}).get("scoreStr"),
        }
        for match in matches
    ]


def process_dataframe(df):
    """
    Process the DataFrame: Map country codes to full names, split the result column,
    and remove unnecessary columns.

    Args:
    - df (pd.DataFrame): The DataFrame containing the fetched league data.

    Returns:
    - pd.DataFrame: The processed DataFrame.
    """

    # Mapping country codes to full names
    country_map = {
        "ESP": "Spain",
        "ENG": "England",
        "GER": "Germany",
        "ITA": "Italy",
        "FRA": "France",
        "NED": "Netherlands",
    }
    df["country"] = df["country"].map(country_map)

    # Splitting result column into home and away scores
    df[["home_score", "away_score"]] = df["result"].str.split(" - ", expand=True)
    df.drop(columns=["result"], inplace=True)

    return df


def generate_date_hours() -> pd.DataFrame:
    """
    Generate a DataFrame with hourly timestamps between two dates.

    Returns:
        DataFrame with hourly timestamps.
    """
    date_hour_range = pd.date_range(start="2023-01-01", end="2024-12-31", freq="H")
    return pd.DataFrame({"date_hour": date_hour_range})


def main() -> pd.DataFrame:
    # Define leagues and target season
    leagues = [
        {"id": 87, "name": "laliga"},
        {"id": 47, "name": "premier-league"},
        {"id": 54, "name": "bundesliga"},
        {"id": 55, "name": "serie-a"},
        {"id": 53, "name": "ligue-1"},
        {"id": 57, "name": "eredivisie"},
    ]

    season = "2023/2024"
    final_data = pd.DataFrame()

    with requests.Session() as session:
        for league in leagues:
            details, matches = get_league_fixtures(session, league["id"], season)

            if details and matches:
                details_dict = extract_details(details)
                matches_dict = extract_matches(matches)

                league_df = pd.DataFrame(matches_dict)
                league_df = league_df.assign(**details_dict)

                final_data = pd.concat([final_data, league_df], ignore_index=True)

    final_data = process_dataframe(final_data)

    return final_data


if __name__ == "__main__":
    matches_df = main()
    date_hours_df = generate_date_hours()

    matches_df.to_csv("./matches.csv", index=False)
    date_hours_df.to_csv("./dates.csv", index=False)
