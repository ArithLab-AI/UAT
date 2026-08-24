from enum import Enum


class LocationType(str, Enum):
    """Location shape the backend detected in the selected location column(s).

    Drives which map chart types are actually renderable: COORDINATES has real
    lat/long points (pin/heatmap/bubble all work); the named-region types have
    no coordinates at all, so only choropleth (matched by name/code on the
    frontend's own boundary data) makes sense — see
    ``geospatial_analysis_service._compute_geospatial``'s chart-type snapping.
    """

    COORDINATES = "coordinates"
    CITY = "city"
    STATE = "state"
    COUNTRY = "country"
    ZIP_CODE = "zip_code"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    REGION = "region"  # generic fallback when no more specific hint matches
