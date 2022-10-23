# Práctica del módulo Big Data Processing

## Estructura de directorios
    * capturas: Capturas de pantalla del directorio donde se almacenan los .parquet y de las tablas con datos.
    * src: Carpeta con los archivos fuente de la aplicación.

## Memoria
    El proyecto está basado en la arquitectura de capas Lambda. realizando las siguientes tareas en cada una de ellas:
    * Streaming:
        En esta capa se obtienen los datos en tiempo real desde un servidor Kafka por un topic determinado. Una vez obtenidos los datos
        se realizan unas métricas que son almacenadas en una BD's PostgreSQL montada en la nube de google. Las tareas tienen el siguiente pipeline:
            * Obtención de los datos desde Kafka
            * Conversion de los binarios kafka para poder realizar la lectura de datos y metricas
            * Obtencion de los datos de usuario desde PostgreSQL
            * Cruce de datos entre los datos de Kafka y los datos de usuario de la BD's
            * Cálculo de las métricas
            * Almacenamiento de los datos crudos en el disco y las métricas en la BD's

    * Batch





    * Batch
