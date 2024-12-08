import threading
import bittensor as bt
from nextplace.protocol import RealEstateSynapse, RealEstatePrediction, RealEstatePredictions
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import NUMBER_OF_PROPERTIES_PER_SYNAPSE
import hashlib
import os
import json

"""
Helper class manages creating Synapse objects
"""

def _generate_uuid_from_sha256() -> str:
    """
    Generate a random UUID based on a SHA-256 hash and return it as a string.

    Returns:
        str: A UUID string created from a SHA-256 hash.
    """
    random_bytes = os.urandom(32)
    sha256_hash = hashlib.sha256(random_bytes).hexdigest()
    return f"{sha256_hash[:8]}-{sha256_hash[8:12]}-{sha256_hash[12:16]}-{sha256_hash[16:20]}-{sha256_hash[20:32]}"


class SynapseManager:

    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def get_synapse(self) -> RealEstateSynapse or None:
        """
        Get a property from the `properties` table, format the synapse
        Returns:
            A RealEstateSynapse to send to Miners, or None
        """

        current_thread = threading.current_thread().name
        try:
            # Query to get the next round of properties
            retrieve_query = f'''
                SELECT * FROM properties
                LIMIT {NUMBER_OF_PROPERTIES_PER_SYNAPSE}
            '''
            property_data = self.database_manager.query(retrieve_query)  # Execute query

            if len(property_data) == 0:
                return None

            nextplace_id_index = 0
            row_ids = [row[nextplace_id_index] for row in property_data]  # Extract unique ID's
            formatted_ids = ','.join(f"'{str(nextplace_id)}'" for nextplace_id in row_ids)
            delete_query = f'''
                    DELETE FROM properties
                    WHERE nextplace_id IN ({formatted_ids})
                '''
            self.database_manager.query_and_commit(delete_query)  # Remove the retrieved rows from the database

            outgoing_data: list[RealEstatePrediction] = []

            for property_datum in property_data:  # Iterate db responses
                if property_datum:
                    try:
                        next_property = self._property_from_database_row(property_datum)
                        outgoing_data.append(next_property)
                    except IndexError as ie:
                        bt.logging.error(f"| {current_thread} | ❗IndexError: {ie} - The data from the database failed to convert to a Synapse")
                        return None
                else:
                    bt.logging.warning(f"| {current_thread} | ❗No property data found in the database")
                    return None

            real_estate_predictions = RealEstatePredictions(predictions=outgoing_data)
            synapse_id = _generate_uuid_from_sha256()
            self._store_synapse_data(outgoing_data, synapse_id)
            synapse = RealEstateSynapse.create(uuid=synapse_id, real_estate_predictions=real_estate_predictions)
            bt.logging.debug(f"| {current_thread} | 🪲 DEBUG Created Synapse: {synapse}")
            market_index = 20
            market = property_data[0][market_index]
            bt.logging.trace(f"| {current_thread} | ✉️ Created Synapse with {len(outgoing_data)} properties in {market} with UUID {synapse_id}")
            return synapse

        except IndexError:
            bt.logging.info(f"| {current_thread} | ❗No property data available")
            return None

    def _store_synapse_data(self, outgoing_prediction_data: list[RealEstatePrediction], synapse_id: str) -> None:
        """
        Store the synapse id along with all nextplace ids
        Args:
            outgoing_prediction_data: all home data for this synapse
            synapse_id: random has for this synapse

        Returns:
            None
        """
        if len(outgoing_prediction_data) == 0:
            return
        nextplace_ids = json.dumps([x.nextplace_id for x in outgoing_prediction_data])  # Extract & serialize nextplace ids in this synapse
        query_str = "INSERT OR REPLACE INTO synapse_ids (uuid, nextplace_ids) VALUES (?, ?)"  # query
        values = (synapse_id, nextplace_ids)  # values
        self.database_manager.query_and_commit_with_values(query_str, values)

    def _property_from_database_row(self, property_data: any) -> RealEstatePrediction:
        """
        Convert API response to a RealEstatePrediction object
        Args:
            property_data: API response

        Returns:
            RealEstatePrediction object
        """
        return RealEstatePrediction(
            nextplace_id=property_data[0],
            property_id=property_data[1],
            listing_id=property_data[2],
            address=property_data[3],
            city=property_data[4],
            state=property_data[5],
            zip_code=property_data[6],
            price=property_data[7],
            beds=property_data[8],
            baths=property_data[9],
            sqft=property_data[10],
            lot_size=property_data[11],
            year_built=property_data[12],
            days_on_market=property_data[13],
            latitude=property_data[14],
            longitude=property_data[15],
            property_type=property_data[16],
            last_sale_date=property_data[17],
            hoa_dues=property_data[18],
            query_date=property_data[19],
            market=property_data[20],
        )
