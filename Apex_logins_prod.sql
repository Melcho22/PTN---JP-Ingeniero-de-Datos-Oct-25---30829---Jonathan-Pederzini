CREATE OR REPLACE TABLE `arcane-tome-333902.prueba_tec.Apex_logins_prod`
(
  Sitio STRING,
  Agente STRING,
  Login TIMESTAMP,
  Logout TIMESTAMP,
  Tiempo_total TIME,
  Username STRING,
  fecha_ingesta DATE,
  Id_Empleado INT64,
  Cuenta INT,
  Fecha_desde DATE,
  Fecha_hasta DATE
)
PARTITION BY fecha_ingesta AS
SELECT DISTINCT
  t1.Sitio,
  t1.Agente,
  CAST(t1.Login AS TIMESTAMP)  AS Login,
  CAST(t1.Logout AS TIMESTAMP) AS Logout,
  CAST(t1.`Tiempo total` AS TIME) AS Tiempo_total,
  t1.Username,
  CAST(t1.fecha_ingesta AS DATE) AS fecha_ingesta,
  t2.Id_Empleado,
  t2.Cuenta,
  CAST(t2.Fecha_desde AS DATE) AS Fecha_desde,
  CAST(t2.Fecha_hasta AS DATE) AS Fecha_hasta
FROM `arcane-tome-333902.prueba_tec.Apex_logins` AS t1
INNER JOIN `arcane-tome-333902.prueba_tec.Nómina` AS t2

  ON t1.Username = t2.Correo_electronico;
