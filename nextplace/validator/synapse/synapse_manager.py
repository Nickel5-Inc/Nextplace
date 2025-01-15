import threading

import bittensor as bt
from nextplace.protocol import RealEstateSynapse, RealEstatePrediction, RealEstatePredictions
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import NUMBER_OF_PROPERTIES_PER_SYNAPSE
from nextplace.validator.forward_web_request.retrieve_web_query import WebsiteRequestsChecker

"""
Helper class manages creating Synapse objects
"""

class SynapseManager:
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager
        self.website_checker = WebsiteRequestsChecker(database_manager)

    def sync_website_data(self) -> bool:
        """Synchronize data from API to database"""
        return self.website_checker.update_from_api()

    def get_synapse(self) -> RealEstateSynapse or None:
        """
        Get a property from the `properties` table, format the synapse
        Returns:
            A RealEstateSynapse to send to Miners, or None
        """
        current_thread = threading.current_thread().name
        
        try:
            # Get user property requests from website
            self.sync_website_data()

            # Query to get the next round of properties
            retrieve_query = f'''
                SELECT * FROM properties
                LIMIT {NUMBER_OF_PROPERTIES_PER_SYNAPSE}
            '''
            property_data = self.database_manager.query(retrieve_query)  # Execute query

            if len(property_data) == 0:
                return None

            # Add data from website_requests table if available
            web_request_data = self.website_checker.fetch_rows(limit=2)
            if web_request_data:
                bt.logging.info(f"| {current_thread} | ✨ Adding {len(web_request_data)} rows from website_requests table")
                property_data.extend(web_request_data)

            # Delete processed rows
            nextplace_id_index = 0
            row_ids = [row[nextplace_id_index] for row in property_data]  # Extract unique ID's
            formatted_ids = ','.join(f"'{str(nextplace_id)}'" for nextplace_id in row_ids)
            delete_query = f'''
                    DELETE FROM properties
                    WHERE nextplace_id IN ({formatted_ids})
                '''
            self.database_manager.query_and_commit(delete_query)  # Remove the retrieved rows from the database

            if web_request_data:
                web_request_ids = [row[0] for row in web_request_data]
                formatted_web_request_ids = ','.join(f"'{str(nextplace_id)}'" for nextplace_id in web_request_ids)
                delete_web_requests_query = f'''
                    DELETE FROM website_requests
                    WHERE nextplace_id IN ({formatted_web_request_ids})
                '''
                self.database_manager.query_and_commit(delete_web_requests_query)

            outgoing_data = []
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
            synapse = RealEstateSynapse.create(real_estate_predictions=real_estate_predictions)
            market_index = 20
            market = property_data[0][market_index]
            bt.logging.trace(f"| {current_thread} | ✉️ Created Synapse with {len(outgoing_data)} properties in {market}")
            return synapse

        except IndexError:
            bt.logging.info(f"| {current_thread} | ❗No property data available")
            return None

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