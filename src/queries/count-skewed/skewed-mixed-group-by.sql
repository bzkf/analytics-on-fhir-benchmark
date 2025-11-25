SELECT
    observation_code_coding.code,
    COUNT(*) AS code_count
FROM fhir.default.observation, UNNEST(observation.code.coding) AS observation_code_coding
WHERE
    observation_code_coding.system = 'http://loinc.org' AND observation_code_coding.code IN (
        -- Hot codes
        '85354-9', '72514-3', '29463-7', '8867-4', '9279-1'
    )
    OR observation_code_coding.code IN (
        -- Rare codes
        '7917-8', '18752-6', '26881-3', '21924-6', '8310-5'
    )
GROUP BY observation_code_coding.code
