import json
import bittensor as bt

from nextplace.protocol import RealEstateSynapse, RealEstatePrediction, RealEstatePredictions
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import NUMBER_OF_PROPERTIES_PER_SYNAPSE
from nextplace.validator.utils.market_manager import MarketManager

"""
Helper class manages creating Synapse objects
"""


class SynapseManager:

    def __init__(self, database_manager: DatabaseManager, market_manager: MarketManager):
        self.database_manager = database_manager
        self.market_manager = market_manager

    def get_synapse(self) -> RealEstateSynapse or None:
        """
        Get a property from the `properties` table, format the synapse
        Returns:
            A RealEstateSynapse to send to Miners, or None
        """

        try:
            # Query to get the next property
            query = f'''
                SELECT * FROM properties
                ORDER BY days_on_market DESC
                LIMIT {NUMBER_OF_PROPERTIES_PER_SYNAPSE} OFFSET {self.market_manager.property_index}
            '''
            with self.database_manager.lock:  # Acquire lock to get properties from database
                property_data = self.database_manager.query(query)  # Execute query
            outgoing_data = []

            for property_datum in property_data:  # Iterate db responses
                if property_datum:
                    try:
                        prediction = self._prediction_from_property_data(property_datum)
                        outgoing_data.append(prediction)
                    except IndexError as ie:
                        bt.logging.error(f"IndexError: {ie} - The data from the database failed to convert to a Synapse")
                        return None
                else:
                    bt.logging.warning("No property data found in the database")
                    return None

            real_estate_predictions = RealEstatePredictions(predictions=outgoing_data)
            synapse = RealEstateSynapse.create(real_estate_predictions=real_estate_predictions)
            bt.logging.trace(f"Created Synapse with {len(outgoing_data)} properties")
            return synapse

        except IndexError:
            bt.logging.info(f"No property data available")
            return None

    def _prediction_from_property_data(self, property_data: any) -> RealEstatePrediction:
        """
        Convert API response to a RealEstatePrediction object
        Args:
            property_data: API response

        Returns:
            RealEstatePrediction object
        """
        return RealEstatePrediction(
            property_id=property_data[0],
            listing_id=property_data[1],
            address=property_data[2],
            city=property_data[3],
            state=property_data[4],
            zip_code=property_data[5],
            price=property_data[6],
            beds=property_data[7],
            baths=property_data[8],
            sqft=property_data[9],
            lot_size=property_data[10],
            year_built=property_data[11],
            days_on_market=property_data[12],
            latitude=property_data[13],
            longitude=property_data[14],
            property_type=property_data[15],
            last_sale_date=property_data[16],
            hoa_dues=property_data[17],
            query_date=property_data[18],
            market=property_data[19],
        )
