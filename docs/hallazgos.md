# REGISTRO DE HALLAZGOS

## Hallazgo 1.- Llaves de integracion y manejo de datos (Discovery)
No existe un ID que relacione los 3 dominios (Billing, CRM y University) 
Se estableció el atributo `email` como la llave maestra de negocio para lograr esta conexion (conectando Leads, Customers y Students). El análisis hecho con Pandas demostro el 0% de coincidencias. Por ende esta relación fue **DESCARTADA**

Posterior a esto se encontro que external_ref <--> student_id y al realizar un cruce `customers.external_ref` con `students.student_id`, se obtuvo un 100% de coincidencia a nivel de llave (5000 registros), pero 0% de coincidencia en atributos personales (nombres, apellidos y correos distintos para el mismo ID). Sin embargo sigue **RELACIONADA**

## Hallazgo 2.- JOINs en el Dominio CRM (Tabla Activities)
Se analizó la tabla `activities` para entender su comportamiento relacional con `contacts` y `opportunities`. El modelo ER indica una cardinalidad de `0..1` (opcional) hacia ambas entidades.
Mediante el perfilado de datos, se comprobó matemáticamente que las actividades están distribuidas en cuatro escenarios distintos: vinculadas a ambos, solo a un contacto, solo a una oportunidad, o ninguna de las dos.