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

## Hallazgo 6.- Fechas inconsistentes en subscriptions
Se identificaron 783 suscripciones cuya fecha de inicio es posterior a la fecha de finalización.
Los registros fueron conservados en Silver y marcados mediante el campo is_temporally_valid = false, con el objetivo de mantener la trazabilidad del dato fuente y permitir su análisis posterior.

## Hallazgo 7.- Consistencia de líneas de factura
La tabla silver.invoice_items contiene 150.000 registros.
Las validaciones realizadas no identificaron:
- Cantidades nulas, negativas o no convertibles.
- Precios unitarios nulos, negativos o no convertibles.
- Diferencias entre line_total y quantity multiplicado por unit_price.
- Relaciones huérfanas con invoices.
- Relaciones huérfanas con products.
Por lo tanto, las líneas individuales de factura presentan consistencia matemática y referencial.
### Facturas sin líneas de detalle
Se identificaron 2.502 facturas de un total de 50.000 que no poseen registros relacionados en silver.invoice_items.
Los registros fueron conservados, ya que no se cuenta con información suficiente para determinar si representan facturas incompletas, cargos generados sin detalle u otra situación del sistema fuente.
### Diferencias entre factura y líneas de detalle
De las 47.498 facturas que poseen líneas de detalle:
- 1 factura presenta coincidencia entre invoices.total y la suma de invoice_items.line_total.
- 47.497 facturas presentan diferencias entre ambos importes.
- La diferencia absoluta promedio es de 629,65.
- La diferencia absoluta máxima identificada es de 5.774,11.

Debido a que las líneas individuales cumplen la relación line_total = quantity × unit_price, la inconsistencia se encuentra entre
el total declarado en invoices y la suma de sus detalles.
No se corrigieron los importes automáticamente, porque los datos fuente no indican si el total de factura incluye impuestos, descuentos, conversiones  u otros.