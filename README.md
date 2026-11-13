# Mercedes-Benz EU Data Act for Home Assistant

Custom Component for **Home Assistant** that consumes real-time telemetry from Mercedes-Benz vehicles and chargers (Wallbox) using the **Third Party Data Access EU Data Act** API (`Push API` over Apache Kafka with `SASL_SSL` / `OAUTHBEARER` authentication).

## Technical Architecture

The Mercedes-Benz **Third Party Data Access EU Data Act** API does not use traditional REST polling for telemetry; instead, it uses a **real-time Push stream over Apache Kafka** (`bootstrap.streaming.connect-business.net:443`).
This integration:

1. Automatically manages obtaining and refreshing **OAuth2 (`client_credentials`)** tokens against the Mercedes-Benz IAM server.
2. Maintains an asynchronous background consumer (`aiokafka`) connected to your dedicated Kafka topic.
3. Dynamically creates devices (by `VIN` or `deviceId`) and entities (`sensor`, `binary_sensor`, `device_tracker`) as signals arrive from your subscribed data packages (`6001` through `6009`).

## Prerequisites in Mercedes-Benz Developer Portal

1. Create a project in [Mercedes-Benz /developers](https://developer.mercedes-benz.com/products/3rd_party_data_access_eu_data_act/details) and subscribe to the **Third Party Data Access EU Data Act** product.
2. Obtain your **API Platform Credentials** (`Client ID`, `Client Secret`, `Scope`) and your assigned **Kafka Topic** name.
3. Activate the *Data Act Service* on the vehicle/wallbox and complete the owner consent flow (*Authorization Code Flow* + calling the *Services API* to enable packages `6001`–`6009`).

## Installation via HACS

1. Upload this repository to your GitHub account.
2. In Home Assistant, open **HACS** -> **Integrations** -> top-right menu (`⋮`) -> **Custom repositories**.
3. Add your GitHub repository URL and select the **Integration** category.
4. Install **Mercedes-Benz EU Data Act** and restart Home Assistant.
5. Go to **Settings** -> **Devices & services** -> **Add integration** -> search for **Mercedes-Benz EU Data Act**.