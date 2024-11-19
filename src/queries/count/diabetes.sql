SELECT COUNT(DISTINCT condition.id)
FROM fhir.default.condition
LEFT JOIN fhir.default.encounter ON condition.encounter.reference = CONCAT('Encounter/', encounter.id)
LEFT JOIN UNNEST(condition.code.coding) AS condition_coding ON TRUE
LEFT JOIN fhir.default.patient AS patient ON encounter.subject.reference = CONCAT('Patient/', patient.id)
WHERE
    DATE(FROM_ISO8601_TIMESTAMP(encounter.period.start)) >= DATE('2020-01-01')
    AND condition_coding.system = 'http://snomed.info/sct'
    AND condition_coding.code IN ('73211009', '427089005', '44054006')
    AND DATE(patient.birthdate) >= DATE('1970-01-01')
