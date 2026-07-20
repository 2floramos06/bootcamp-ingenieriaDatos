# REGISTRO DE HALLAZGOS

## Hallazgo 1.- Llaves de integracion y manejo de datos (Discovery)
No existe un ID que relacione los 3 dominios (Billing, CRM y University) 
Se estableció el atributo `email` como la llave maestra de negocio para lograr esta conexion (conectando Leads, Customers y Students). El análisis hecho con Pandas demostro el 0% de coincidencias. Por ende esta relación fue **DESCARTADA**

Posterior a esto se encontro que external_ref <--> student_id y al realizar un cruce `customers.external_ref` con `students.student_id`, se obtuvo un 100% de coincidencia a nivel de llave (5000 registros), pero 0% de coincidencia en atributos personales (nombres, apellidos y correos distintos para el mismo ID). Sin embargo sigue **RELACIONADA**

## Hallazgo 2.- JOINs en el Dominio CRM (Tabla Activities)
Se analizó la tabla `activities` para entender su comportamiento relacional con `contacts` y `opportunities`. El modelo ER indica una cardinalidad de `0..1` (opcional) hacia ambas entidades.
Mediante el perfilado de datos, se comprobó matemáticamente que las actividades están distribuidas en cuatro escenarios distintos: vinculadas a ambos, solo a un contacto, solo a una oportunidad, o ninguna de las dos.

## Hallazgo 3.- Hallazgo sobre subscriptions
Se identificó en el notebook que suscripciones cuya fecha de inicio es posterior a su fecha de finalización. Esto contradice la secuencia temporal esperada de una suscripción.

## Hallazgo 4.- Hallazgo sobre opoortunities
Se identificó en el notebook que oportunidades cuya fecha de creación es posterior a la fecha de cierre. Este resultado puede representar inconsistencias ya que una oportunidad comercial debería haberse creado antes o el mismo día de su cierre.

## Hallazgo 5.- Hallazgos de UNIVERSITY

### Inscripciones repetidas
Se identificaron 46 registros correspondientes a 23 combinaciones repetidas de student_id, course_id y semester_id.
Los registros no se borraron porque pueden presentar distintos estados, como: active, dropped, failed o completed. Estas diferencias podrían representar reinscripciones o cambios en el estado académico.

### Integridad de las ponderaciones
De las inscripciones que tienen notas:
- 140 tienen pesos que suman exactamente 1. (100%)
- 22.646 tienen pesos que no suman exactamente 1. (100%)
