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
En UNIVERSITY se creó la bandera is_weight_sum_valid valida solo si las ponderaciones de las notas entre homework, project, midterm, quiz y final completan el 100%, no determina si un estudiante aprobó o reprobó.

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