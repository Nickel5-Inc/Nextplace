from typing import TypedDict, Optional


class Home(TypedDict):
    nextplace_id: Optional[str]
    property_id: Optional[str]
    listing_id: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    price: Optional[int]
    beds: Optional[int]
    baths: Optional[float]
    sqft: Optional[int]
    lot_size: Optional[int]
    year_built: Optional[int]
    days_on_market: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]
    property_type: Optional[str]
    last_sale_date: Optional[str]
    hoa_dues: Optional[int]
