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

## Hallazgo 8.- Hallazgo sobre los pesos de las evaluaciones
Se encontraron inscripciones con sumas de pesos desde 0.10 hasta valores superiores a 3.00.  Esto indica que los pesos no deben interpretarse obligatoriamente como porcentajes cuya suma siempre sea 100%. También pueden representar ponderaciones relativas o una característica particular del sistema de origen.

Por esta razón, la suma diferente de 1 no se utiliza para descartar las notas. Se conserva como indicador descriptivo y el análisis utiliza el promedio ponderado de las evaluaciones disponibles.

## Hallazgo 9.- Hallazgos de Billing en Gold
- 9.933 de los 10.000 clientes tienen facturas.
- Se identificaron ocho monedas distintas.
- La tabla Gold financiera contiene 32.147 combinaciones cliente-moneda.
- Existen 2.502 facturas sin items.
- De las 47.498 facturas con items, casi todas presentan diferencias entre el total de factura y la suma de sus líneas.
- Existen 783 suscripciones con fechas temporalmente inconsistentes.
- 2.224 clientes no tienen suscripciones.

## Hallazgo 10.- Conciliación entre facturas y pagos
Al comparar el total de cada factura con la suma de sus pagos se identificaron:
- 20.482 facturas sobrepagadas.
- 8 facturas pagadas exactamente.
- 29.510 facturas subpagadas.
- 14.476 facturas con estado "paid" cuyo pago acumulado es menor que el total de la factura.
- No se encontraron facturas "pending" u "overdue" completamente pagadas.
Estos resultados evidencian una falta de conciliación entre los importes de pagos y el estado de las facturas en el sistema de origen.

## Hallazgo 11.- Productos y suscripciones
Los 200 productos tienen suscripciones y registros en invoice_items. Se identificaron 30 productos inactivos y 1.753 suscripciones activas
asociadas a productos inactivos. Esta situación se conserva como indicador para revisión, ya que el dataset no permite determinar si corresponde a una inconsistencia o a productos retirados que continúan disponibles para clientes antiguos.