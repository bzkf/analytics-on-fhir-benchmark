SELECT
    id AS patient_id,
    birthdate AS patient_birthdate,
    gender AS patient_gender
FROM fhir.default.patient
WHERE gender = 'female' AND date(birthdate) >= date('1970-01-01')
ORDER BY patient.id ASC
