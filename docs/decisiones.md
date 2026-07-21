# REGISTRO DE DECISIONES

## Decision 1.- Creación del modelo ER (Discovery)
Los 18 CSVs (6 de billing, 6 de crm y 6 de university) no contaban con un modelo ER formal 
Por ende se tomo la **DECISIÓN** de realizar un perfilado manual de los 18 archivos utilizando Python (Pandas .info() y .columns()) para inferir la cardinalidad, identificar PKs y FKs 
Se construyó el modelo ER desde cero en dbdiagram.io de acuerdo a los resultados obtenidos con pandas, agrupando las entidades en los 3 dominios correspondientes (Billing, CRM Y University)

## Decision 2.- Relación de negocio external_ref con student_id (Discovery)
Si bien coinciden los IDs pero no tienen los mismos datos personales de las personas, en base al análisis se pudo concluir que "Customers" hace referencia a la persona que paga la suscripción del estudiante (ej. Como un papá pagando el colegio de su hijo)

## Decisión 3.- Conservación de los datos originales
Durante Discovery no se modificaron ni eliminaron registros. Los problemas e inconsistencias encontradas serán tratados en la capa Silver mediante conversiones, estandarizaciones, manteniendo la trazabilidad con los archivos originales.

## Decisión 4.- Tipos de datos en Silver
Se decidió convertir las columnas temporales actualmente alamcenadas como object (texto) a "DATE" o "TIMESTAMP".
Los IDs permanecen como texto, los conteos como "INTEGER", los atributos lógicos como "BOOLEAN" y los montos como "NUMERIC"

## Decisión 5.- Bandera de peso (weight)
En UNIVERSITY se creó la bandera is_weight_sum_one valida solo si las ponderaciones de las notas entre homework, project, midterm, quiz y final estan cerca de 1 (ya que se notó que no siempre es 1, sino que puede ser mayor o menor). 
Por este motivo, no se considera que una suma diferente de 1 invalide automáticamente las notas. Los pesos se interpretan como ponderaciones relativas entre las evaluaciones disponibles. SUM(score * weight) / SUM(weight), este indicador representa las evaluaciones registradas y no se presenta como una nota final oficial. No se coloca 0 a las evaluaciones ausentes, porque el dataset no permite diferenciar entre una actividad no realizada, no registrada o pendiente. La columna is_weight_sum_one se conserva como un indicador descriptivo, no como una condición de validez.

## Decisión 6.- Determinación del resultado académico
Para identificar las inscripciones reprobadas se utilizará el campo status de la tabla enrollments, debido a que representa el resultado registrado directamente por el sistema fuente.

La interpretación será:
- failed: inscripción reprobada.
- dropped: inscripción retirada (abandono).
- active: inscripción actualmente activa.
- completed: inscripción finalizada.

No se calculará la aprobación o reprobación utilizando un umbral de 51 puntos, ya que los datos fuente no especifican una nota mínima de aprobación y el cálculo ponderado puede presentar diferencias respecto al estado oficial.

## Decisión 7.- Diferencias entre facturas y sus detalles
Los campos invoices.total e invoice_items.line_total se conservarán con los valores recibidos desde la fuente.
No se reemplazará invoices.total por la suma de las líneas, debido a que no se dispone de reglas suficientes para determinar cuál de los importes es el valor oficial.
La diferencia será expuesta como indicador de calidad y se analizará posteriormente en la capa Gold.

## Decisión 8.- Detalles de Gold Billing
Se identificaron ocho monedas en las facturas y la mayoría de los clientes con facturación presenta operaciones en más de una moneda.
Por esta razón, el resumen financiero utiliza una fila por cliente y moneda. No se suman importes entre monedas diferentes ni se aplican tipos de cambio no proporcionados por el origen.
Las suscripciones se resumen por cliente en una tabla separada. 

## Decisión 9.- Tratamiento de saldos y sobrepagos
Los pagos se conservan tal como fueron registrados en el origen. No se modifica el estado de las facturas a partir del monto pagado.
En Gold se separan:
- saldo neto
- monto pendiente
- monto pagado en exceso
- clasificación de facturas sobrepagadas, pagadas exactamente y subpagadas
- inconsistencias entre el estado "paid" y el pago acumulado
Los sobrepagos no compensan los montos pendientes en los indicadores de deuda.
Las suscripciones activas en productos inactivos se presentan como casos para revisión y no se eliminan ni se reclasifican automáticamente.

## Decisión 10.- CRM en Gold
El resumen comercial utiliza una fila por cuenta. Las métricas de contactos, oportunidades, relaciones y actividades se agregan antes
de realizar las uniones para evitar duplicar oportunidades o montos.
Cuando una actividad tiene una oportunidad válida, se asigna a la cuenta de esa oportunidad. Cuando solo tiene un contacto válido, se
asigna a la cuenta del contacto. Las actividades sin contacto ni oportunidad no se asignan artificialmente a una cuenta.
Los leads se analizan en una tabla separada porque el origen no proporciona una relación entre leads, accounts, contacts u opportunities.
El campo "amount" de oportunidades no incluye moneda. Por este motivo, los valores se presentan como montos registrados en CRM y no se
etiquetan como USD ni se combinan con los importes de Billing.