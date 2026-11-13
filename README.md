# Mercedes-Benz EU Data Act for Home Assistant

Integración personalizada (*Custom Component*) para **Home Assistant** que consume telemetría en tiempo real de vehículos y cargadores (Wallbox) Mercedes-Benz mediante la API **Third Party Data Access EU Data Act** (`Push API` sobre Apache Kafka con autenticación `SASL_SSL` / `OAUTHBEARER`).

## Arquitectura Técnica

La API **Third Party Data Access EU Data Act** de Mercedes-Benz no utiliza *polling* REST tradicional para telemetría, sino un flujo **Push en tiempo real sobre Apache Kafka** (`bootstrap.streaming.connect-business.net:443`).
Esta integración:

1. Gestiona automáticamente la obtención y renovación de tokens **OAuth2 (`client_credentials`)** contra el servidor IAM de Mercedes-Benz.
2. Mantiene un consumidor asíncrono (`aiokafka`) en segundo plano conectado a tu tópico exclusivo de Kafka.
3. Crea dinámicamente dispositivos (por `VIN` o `deviceId`) y entidades (`sensor`, `binary_sensor`, `device_tracker`) conforme llegan señales de los paquetes contratados (`6001` a `6009`).

## Prerrequisitos en Mercedes-Benz Developer Portal

1. Crear un proyecto en [Mercedes-Benz /developers](https://developer.mercedes-benz.com/products/3rd_party_data_access_eu_data_act/details) y suscribirte al producto **Third Party Data Access EU Data Act**.
2. Obtener tus **API Platform Credentials** (`Client ID`, `Client Secret`, `Scope`) y el nombre de tu **Kafka Topic**.
3. Activar el servicio *Data Act Service* en el vehículo/wallbox y completar el flujo de consentimiento del propietario (*Authorization Code Flow* + llamada al *Services API* para activar los paquetes `6001`–`6009`).

## Instalación mediante HACS

1. Sube este repositorio a tu cuenta de GitHub.
2. En Home Assistant, abre **HACS** -> **Integraciones** -> menú superior derecho (`⋮`) -> **Repositorios personalizados**.
3. Añade la URL de tu repositorio de GitHub y selecciona la categoría **Integration**.
4. Instala **Mercedes-Benz EU Data Act** y reinicia Home Assistant.
5. Ve a **Ajustes** -> **Dispositivos y servicios** -> **Añadir integración** -> busca **Mercedes-Benz EU Data Act**.
