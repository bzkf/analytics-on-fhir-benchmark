SELECT COUNT(*)
FROM fhir.default.observation, UNNEST(observation.code.coding) AS observation_code_coding
WHERE observation_code_coding.system = 'http://loinc.org' AND observation_code_coding.code IN ('7917-8','18752-6','26881-3','21924-6','8310-5','72091-2','71934-4','72092-0','22577-1','61576-5')
