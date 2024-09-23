# Next Place Miner

The examples in the README assume you are using `PM2` to run you miner.

## How the Miner works
A miner will receive requests from a validator and respond with a predicted sale price and sale date. The dataset used by Validators can be found at INSERT URL HERE if you wish to call API's for more data on each property and develop a more complex model. The dataset is constantly updating based on new homes that are listed and homes that are sold. A prediction must be made before the listed sale date; just because a home is in the database does *not* mean that it is still on the market; the homes sale can have a slight delay in entering our dataset.

## Clone the Repo
```
git clone https://github.com/Nickel5-Inc/Nextplace.git
cd nextplace
```

## Install System Dependencies
Create a new virtual environment. We recommend python version 3.10. You may need to install this.
```
python3 -m venv venv
```
Activate the virtual environment
```
source venv/bin/activate
```
Install dependencies
```
pip install -r requirements.txt
```
Install package
```
pip install -e .
```

## Register hotkey
```
btcli subnets register --netuid <UID> --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

## Using the Base Statistical Model
You can run this command to use the base statistical model
```
pm2 start --name "statistical-miner" python -- ./neurons/miner.py --wallet.name <your_wallet> --wallet.hotkey <your_hotkey> --logging.trace --subtensor.chain_endpoint <chain_endpoint> --subtensor.network <network> --axon.port 8092 --model.source hugging_face --model_path Nickel5HF/NextPlace --model_filename StatisticalBaseModel.py
```

## Using the Base Machine Learning Model
You can run this command to use the base machine learning model
```
pm2 start --name "ml-miner" python -- ./neurons/miner.py --wallet.name <your_wallet> --wallet.hotkey <your_hotkey> --logging.trace --subtensor.chain_endpoint <chain_endpoint> --subtensor.network <network> --axon.port 8093 --model.source hugging_face --model_path Nickel5HF/NextPlace --model_filename MLBaseModel.py
```

## Using a Custom Model
In order to use a custom model you need to create a Python class for that model. You can store the file either on your 
local machine or on [Hugging Face](https://huggingface.co/). We recommend uploading your model to a private Hugging Face repository.

## Markets in NextPlace
Have a look at our [Markets](../../nextplace/validator/market/markets.py). This will help inform which markets you should
expect to make predictions for.

### Custom Model Class Structure
Your custom model must be defined in a Python class. This Python class must reside within a file whose name matches the
Python class. For example, if your class is defined like this `class CustomNextPlaceModel` it must reside within a
file called `CustomNextPlaceModel.py`. Furthermore, this class _must_ implement a method `def run_inference(input_data)`.
Have a look at the following example class for how to spec this method.

Example: `BaseModel.py`
```
from typing import Tuple, TypedDict, Optional
import datetime


class BaseModel:

    def __init__(self):
        self._load_model()

    def _load_model(self):
        """
        Perform any actions needed to load the model.
        EX: Establish API connections, download an ML model for inference, etc...
        """
        print("Loading model...")
        # Optional model loading
        print("Model loaded.")

    def _sale_date_predictor(self, days_on_market: int):
        """
        Calculate the expected sale date based on the national average
        :param days_on_market: number of days this house has been on the market
        :return: the predicted sale date, based on the national average of 34 days
        """
        national_average = 34
        if days_on_market < national_average:
            days_until_sale = national_average - days_on_market
            sale_date = datetime.date.today() + datetime.timedelta(days=days_until_sale)
            return sale_date
        else:
            return datetime.date.today() + datetime.timedelta(days=1)

    def run_inference(self, input_data: ProcessedSynapse) -> Tuple[float, str]:
        """
        This example just uses the listing price as the predicted sale price, and the national average for the predicted sale date.
        Predict the sale price and sale date for the house represented by `input_data`
        :param input_data: a formatted Synapse from the validator, representing a currently listed house
        :return: the predicted sale price and predicted sale date for this home
        """
        predicted_sale_price = float(input_data['price']) if ('price' in input_data) else 1.0
        predicted_sale_date = self._sale_date_predictor(input_data['days_on_market']) if ('days_on_market' in input_data) else datetime.date.today() + datetime.timedelta(days=1)
        predicted_sale_date = predicted_sale_date.strftime("%Y-%m-%d")
        print(f"Predicted sale price: {predicted_sale_price}")
        print(f"Predicted sale date: {predicted_sale_date}")
        return predicted_sale_price, predicted_sale_date
        

class ProcessedSynapse(TypedDict):
    id: Optional[str]
    nextplace_id: Optional[str]
    property_id: Optional[str]
    listing_id: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    price: Optional[float]
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
    hoa_dues: Optional[float]
    query_date: Optional[str]
```
NOTE: Many of these fields in the `ProcessedSynapse` object may be `null`. This data is coming from the Validator, 
sourced from the Redfin API. It is advisable to check for `null` values before trying to process any data associated 
with these fields.

### Arguments to the Miner
There are several arguments to the Miner.

#### --force_update_past_predictions [ true | false ]
- This argument defines whether to force the validator to update past predictions that the miner has already made.
- If you set this to `true`, the validator will update predictions you have already made each time a prediction
  for that same property is made. This _may_ result in some predictions being ignored during scoring, because it is
  possible that the house sells _before_ the prediction is updated, making the updated prediction invalid (we don't
  score predictions made on a house after the house sells).

#### --model_source [ hugging_face | local ]
- This argument defines whether you want to load a model from Hugging Face or your local filesystem.

#### --model_filename [ string ]
- This argument defines the name of the file containing the model you'd like to use.

#### --model_path [ string ]
- This argument defines the path to the file containing your model.
- IF using `--model_source local`
  - This is a filepath from `current working directory`
- IF using `--model_source hugging_face`
  - This is `<hugging_face_account>/<repo_id>`

#### --hugging_face_api_key [ string ]
- If you are loading a model from a _private_ Hugging Face repo, put your hugging face token here


### Examples

#### Run a private Hugging Face model
```
pm2 start --name "private-miner" python -- ./neurons/miner.py --wallet.name <wallet> --wallet.hotkey <hotkey> --logging.trace --subtensor.chain_endpoint <chain_endpoint> --subtensor.network <network> --axon.port <port_number> --model_source hugging_face --model_path <your_hugging_face_account_name>/<private_hugging_face_repo> --model_filename <filename> --hugging_face_api_key <your_hugging_face_token>
```

#### Run a model from your filesystem
```
pm2 start --name "local-miner" python -- ./neurons/miner.py --wallet.name <wallet> --wallet.hotkey <hotkey> --logging.trace --subtensor.chain_endpoint <chain_endpoint> --subtensor.network <network> --axon.port <port_number> --model_source local --model_path ../../ --model_filename BaseModel.py
```
This loads a file called `BaseModel.py` from the local filesystem. In this example, the file structure looks like this
```
- root
  - PycharmProjects
    - nextplace
- usr
- BaseModel.py
- bin
```
- NOTE: The `--model_path` argument in this example is `../../` because we are using the `current working directory` to build
the full path to the file. The current working directory is `/root/PycharmProjects/nextplace`, so we must go up two levels
to `/root` directory. The `--model_filename` argument is `BaseModel.py`. We can see in the example file structure that
`BaseModel.py` is in the `/root` directory.
