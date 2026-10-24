"""Geographic utilities and global city reference coordinates."""

import math

from models.transaction import GeoLocation

# Major worldwide metropolitan hubs for realistic geo-fraud simulation
GLOBAL_CITIES = [
    GeoLocation(city="New York", country="USA", latitude=40.7128, longitude=-74.0060),
    GeoLocation(city="London", country="UK", latitude=51.5074, longitude=-0.1278),
    GeoLocation(city="Tokyo", country="Japan", latitude=35.6762, longitude=139.6503),
    GeoLocation(city="Paris", country="France", latitude=48.8566, longitude=2.3522),
    GeoLocation(city="San Francisco", country="USA", latitude=37.7749, longitude=-122.4194),
    GeoLocation(city="Sydney", country="Australia", latitude=-33.8688, longitude=151.2093),
    GeoLocation(city="Singapore", country="Singapore", latitude=1.3521, longitude=103.8198),
    GeoLocation(city="Dubai", country="UAE", latitude=25.2048, longitude=55.2708),
    GeoLocation(city="Berlin", country="Germany", latitude=52.5200, longitude=13.4050),
    GeoLocation(city="Toronto", country="Canada", latitude=43.6532, longitude=-79.3832),
]


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two GPS points in kilometers."""
    r = 6371.0  # Earth's radius in kilometers

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c
