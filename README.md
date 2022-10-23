# Práctica del módulo Big Data Processing

## Estructura de directorios
- capturas: Capturas de pantalla del directorio donde se almacenan los .parquet y de las tablas con datos.
- src: Carpeta con los archivos fuente de la aplicación.

## Memoria
El proyecto está basado en la arquitectura de capas Lambda. Se recogen datos en tiempo real de las antenas de móviles
para posteriormente curzarlos con datos de clientes y analizaros a través de diferentes métricas. Las capas principales son:
    - Streaming:
        En esta capa se obtienen los datos en tiempo real desde un servidor Kafka por un topic determinado. Una vez obtenidos los datos
        se realizan unas métricas que son almacenadas en una BD's PostgreSQL montada en la nube de google. Las tareas tienen el siguiente pipeline:
            
- Obtención de los datos desde Kafka
  - Conversion de los binarios kafka para poder realizar la lectura de datos y metricas
  - Obtencion de los datos de usuario desde PostgreSQL
  - Cruce de datos entre los datos de Kafka y los datos de usuario de la BD's
  - Cálculo de las métricas (Datos consumisdos por usuario, antena y aplicación) 
  - Almacenamiento de los datos "en crudo" en el disco y las métricas en la BD's

- Batch
  - Obtención de los datos "en crudo" del disco
  - Obtención de los datos del usuario
  - Cálculo de métricas
  - Almacenamiento de las métricas en BD's

- Servicio
En esta capa se realizará la explotación de las métricas calculas y alamacenadas en la BD's


