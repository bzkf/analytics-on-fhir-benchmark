SELECT
    patient.id AS patient_id,
    patient.birthdate AS patient_birthdate,
    observation.id AS observation_id,
    observation_code_coding.code AS loinc_code,
    valuequantity.code AS value_quantity_ucum_code,
    valuequantity.value AS value_quantity_value,
    observation.effectivedatetime AS effective_datetime,
    observation.subject.reference AS observation_patient_reference
FROM fhir.default.observation
LEFT JOIN fhir.default.patient ON observation.subject.reference = CONCAT('Patient/', patient.id)
LEFT JOIN UNNEST(observation.code.coding) AS observation_code_coding ON TRUE
WHERE
    observation_code_coding.system = 'http://loinc.org'
    AND valuequantity.system = 'http://unitsofmeasure.org'
    AND (
        observation_code_coding.code = '718-7'
        AND valuequantity.code = 'g/dL'
        AND valuequantity.value > 25.0
    )
    OR (
        observation_code_coding.code IN ('17856-6', '4548-4', '4549-2')
        AND valuequantity.code = '%'
        AND valuequantity.value > 5
    )
ORDER BY patient.id ASC
