import bittensor as bt
from typing import Optional, List
from pydantic import BaseModel, Field


class RealEstatePrediction(BaseModel):
    """Real Estate Prediction data class"""
    id: Optional[str] = Field(None, description="UUID of the prediction")
    nextplace_id: Optional[str] = Field(None, description="Internal ID for the property")
    property_id: Optional[str] = Field(None, description="ID of the property")
    listing_id: Optional[str] = Field(None, description="ID of the listing")
    address: Optional[str] = Field(None, description="Address of the property")
    city: Optional[str] = Field(None, description="City of the property")
    state: Optional[str] = Field(None, description="State of the property")
    zip_code: Optional[str] = Field(None, description="ZIP code of the property")
    price: Optional[float] = Field(None, description="Current price of the property")
    beds: Optional[int] = Field(None, description="Number of bedrooms")
    baths: Optional[float] = Field(None, description="Number of bathrooms")
    sqft: Optional[int] = Field(None, description="Square footage of the property")
    lot_size: Optional[int] = Field(None, description="Lot size of the property")
    year_built: Optional[int] = Field(None, description="Year the property was built")
    days_on_market: Optional[int] = Field(None, description="Number of days on the market")
    latitude: Optional[float] = Field(None, description="Latitude of the property")
    longitude: Optional[float] = Field(None, description="Longitude of the property")
    property_type: Optional[str] = Field(None, description="Type of the property")
    last_sale_date: Optional[str] = Field(None, description="Date of the last sale")
    hoa_dues: Optional[float] = Field(None, description="HOA dues")
    query_date: Optional[str] = Field(None, description="Date of the query")
    market: Optional[str] = Field(None, description="The real estate market this property belongs to")
    force_update_past_predictions: Optional[bool] = Field(None, description="Force update past predictions")
    predicted_sale_price: Optional[float] = Field(None, description="Predicted sale price")
    predicted_sale_date: Optional[str] = Field(None, description="Predicted sale date")

class RealEstatePredictions(BaseModel):
    predictions: List[RealEstatePrediction] = Field(None, description="List of predictions")

class RealEstateSynapse(bt.Synapse):
    """Real Estate Synapse class"""
    real_estate_predictions: RealEstatePredictions

    @classmethod
    def create(cls, real_estate_predictions: RealEstatePredictions = None):
        return cls(real_estate_predictions=real_estate_predictions)

    def deserialize(self):
        return self.real_estate_predictions
