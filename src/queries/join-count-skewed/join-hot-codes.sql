SELECT COUNT(DISTINCT patient.id)
FROM fhir.default.Observation observation
JOIN UNNEST(observation.code.coding) AS observation_code_coding ON TRUE
JOIN fhir.default.Patient patient ON observation.subject.reference = CONCAT('Patient/', patient.id)
WHERE observation_code_coding.system = 'http://loinc.org'
    AND  observation_code_coding.code IN ('85354-9', '72514-3', '29463-7', '8867-4', '9279-1')
