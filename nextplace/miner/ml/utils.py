from typing import Union
from nextplace.protocol import RealEstatePrediction


def prepare_input(prediction: RealEstatePrediction) -> dict[str, Union[str, int, float]]:
    """
    Convert the synapse object into a comma-separated string.

    Args:
        prediction (RealEstatePrediction): The input data from the validator.

    Returns:
        dict[str, Union[str, int, float]]: The input for the model.
    """
    return {
        "id": prediction.id,
        "nextplace_id": prediction.nextplace_id,
        "property_id": prediction.property_id,
        "listing_id": prediction.listing_id,
        "address": prediction.address,
        "city": prediction.city,
        "state": prediction.state,
        "zip_code": prediction.zip_code,
        "price": prediction.price,
        "beds": prediction.beds,
        "baths": prediction.baths,
        "sqft": prediction.sqft,
        "lot_size": prediction.lot_size,
        "year_built": prediction.year_built,
        "days_on_market": prediction.days_on_market,
        "latitude": prediction.latitude,
        "longitude": prediction.longitude,
        "property_type": prediction.property_type,
        "last_sale_date": prediction.last_sale_date,
        "hoa_dues": prediction.hoa_dues,
        "query_date": prediction.query_date,
        "market": prediction.market,
    }
