SELECT COUNT(*)
FROM fhir.default.observation, UNNEST(observation.code.coding) AS observation_code_coding
WHERE observation_code_coding.system = 'http://loinc.org' AND observation_code_coding.code IN ('85354-9','72514-3','29463-7','8867-4', '9279-1')
