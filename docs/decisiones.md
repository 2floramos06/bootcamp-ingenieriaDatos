# REGISTRO DE DECISIONES

## Decision 1.- Creación del modelo ER (Discovery)
Los 18 CSVs (6 de billing, 6 de crm y 6 de university) no contaban con un modelo ER formal 
Por ende se tomo la **DECISIÓN** de realizar un perfilado manual de los 18 archivos utilizando Python (Pandas .info() y .columns()) para inferir la cardinalidad, identificar PKs y FKs 
Se construyó el modelo ER desde cero en dbdiagram.io de acuerdo a los resultados obtenidos con pandas, agrupando las entidades en los 3 dominios correspondientes (Billing, CRM Y University)
