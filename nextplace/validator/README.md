# Validator

## API Key
You need a [Redfin API](https://rapidapi.com/ntd119/api/redfin-com-data) key to run this validator. Select the MEGA subscription at $150 per month.
You must store your API key in a `.env` file at the root of this repository, in a field called `NEXT_PLACE_REDFIN_API_KEY`
- EX `.env.sh`
- ```NEXT_PLACE_REDFIN_API_KEY="your-api-key"```

## Setup Steps
Note: Runpod and Vast are not recommended. Validating this subnet does not require a GPU.

## Clone the Repo
```
git clone https://github.com/INSERTOWNER/nextplace.git
cd nextplace
```

## Install System Dependencies

INSERT DEPENDENCIES

## Register hotkey
```
btcli subnets register --netuid <UID> --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

## Start validator
NOTE: trace logging is recommended
```
pm2 start "python neurons/validator.py --wallet.name  <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY> --netuid <UID> --logging.trace"
```


