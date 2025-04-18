# Validator

## API Keys
You need a [Redfin API](https://rapidapi.com/ntd119/api/redfin-com-data) key for the US and a [Redfine API](https://rapidapi.com/ntd119/api/redfin-canada) key for Canada to run this validator. Select the ULTRA subscription for both, at $35 per month.
You must store your API key in a `.env` file at the root of this repository, in a field called `NEXT_PLACE_REDFIN_API_KEY` for US markets, and `NEXTPLACE_CANADA_API_KEY` for Canada. This may be the same key for both, but it should still be stored in two separate variables.
- EX `.env`
- ```NEXT_PLACE_REDFIN_API_KEY="your-api-key"```
- ```NEXTPLACE_CANADA_API_KEY="your-api-key"```

## Setup Steps
Note: Runpod and Vast are not recommended. Validating this subnet does not require a GPU.

## Clone the Repo
```
git clone https://github.com/Nickel5-Inc/Nextplace.git
cd Nextplace
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
pip install -e .
```

## Register hotkey
```
btcli subnets register --netuid 48 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

## Start validator
NOTE: trace logging is recommended
```
pm2 start "python neurons/validator.py --wallet.name  <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY> --netuid 48 --logging.trace"
```


