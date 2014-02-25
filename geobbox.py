# code to create a bounding box from a center geocoord and a 'radius' (half-side of square) in km.
# this is copypasta from SO question at http://stackoverflow.com/questions/3182260/python-geocode-filtering-by-distance
# answered by http://stackoverflow.com/users/84270/john-machin

from math import sin, cos, asin, sqrt, degrees, radians

Earth_radius_km = 6371.0
RADIUS = Earth_radius_km

def haversine(angle_radians):
    return sin(angle_radians / 2.0) ** 2

def inverse_haversine(h):
    return 2 * asin(sqrt(h)) # radians

def distance_between_points(lat1, lon1, lat2, lon2):
    # all args are in degrees
    # WARNING: loss of absolute precision when points are near-antipodal
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    dlat = lat2 - lat1
    dlon = radians(lon2 - lon1)
    h = haversine(dlat) + cos(lat1) * cos(lat2) * haversine(dlon)
    return RADIUS * inverse_haversine(h)

def bounding_box(lat, lon, distance):
    # Input and output lats/longs are in degrees.
    # Distance arg must be in same units as RADIUS.
    # Returns (dlat, dlon) such that
    # no points outside lat +/- dlat or outside lon +/- dlon
    # are <= "distance" from the (lat, lon) point.
    # Derived from: http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
    # WARNING: problems if North/South Pole is in circle of interest
    # WARNING: problems if longitude meridian +/-180 degrees intersects circle of interest
    # See quoted article for how to detect and overcome the above problems.
    # Note: the result is independent of the longitude of the central point, so the
    # "lon" arg is not used.
    dlat = distance / RADIUS
    dlon = asin(sin(dlat) / cos(radians(lat)))
    return degrees(dlat), degrees(dlon)

if __name__ == "__main__":

    # Examples from Jan Matuschek's article

    def test(lat, lon, dist):
        print "test bounding box", lat, lon, dist
        dlat, dlon = bounding_box(lat, lon, dist)
        print "dlat, dlon degrees", dlat, dlon
        print "lat min/max rads", map(radians, (lat - dlat, lat + dlat))
        print "lon min/max rads", map(radians, (lon - dlon, lon + dlon))

    print "liberty to eiffel"
    print distance_between_points(40.6892, -74.0444, 48.8583, 2.2945) # about 5837 km
    print
    print "calc min/max lat/lon"
    degs = map(degrees, (1.3963, -0.6981))
    test(*degs, dist=1000)
    print
    degs = map(degrees, (1.3963, -0.6981, 1.4618, -1.6021))
    print degs, "distance", distance_between_points(*degs) # 872 km