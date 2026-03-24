import IP2Location
from config.base import IP2LOCATION_DB

# Global IP2Location instance for worker processes
_ip2loc = None

def lookup_ip(ip):
    """
    Convert a single IP address to a location dict using IP2Location.
    This function is designed to be used in a multiprocessing environment.
    """
    global _ip2loc
    if _ip2loc is None:
        # Khởi tạo instance duy nhất cho mỗi process worker
        _ip2loc = IP2Location.IP2Location(IP2LOCATION_DB)
    
    record = _ip2loc.get_all(ip)
    
    return {
        "ip": ip,
        "country": record.country_long,
        "region": record.region,
        "city": record.city,
        "latitude": record.latitude,
        "longitude": record.longitude,
    }

def lookup_ip_from_doc(doc):
    """
    Wrapper for ProcessPoolExecutor (lambdas are not picklable).
    Expects doc to have an _id field containing the IP address.
    """
    return lookup_ip(doc["_id"])
