SELECT COUNT(DISTINCT patient.id)
FROM fhir.default.patient
WHERE patient.gender = 'female' AND DATE(patient.birthdate) >= DATE('1970-01-01')
