SELECT
    condition.id AS condition_id,
    condition_coding.code AS condition_snomed_code,
    condition.onsetdatetime AS condition_onset,
    encounter.id AS encounter_id,
    encounter.period.start AS encounter_period_start,
    encounter.period."end" AS encounter_period_end,
    encounter.status AS encounter_status,
    patient.id AS patient_id,
    patient.birthdate AS patient_birthdate
FROM fhir.default.condition AS condition
LEFT JOIN fhir.default.encounter ON condition.encounter.reference = CONCAT('Encounter/', encounter.id)
LEFT JOIN UNNEST(condition.code.coding) AS condition_coding ON TRUE
LEFT JOIN fhir.default.patient AS patient ON encounter.subject.reference = CONCAT('Patient/', patient.id)
WHERE
    DATE(FROM_ISO8601_TIMESTAMP(encounter.period.start)) >= DATE('2020-01-01')
    AND condition_coding.system = 'http://snomed.info/sct'
    AND condition_coding.code IN ('73211009', '427089005', '44054006')
    AND DATE(patient.birthdate) >= DATE('1970-01-01')
ORDER BY patient.id ASC
