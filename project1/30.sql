SELECT P.name
FROM Pokemon AS P
WHERE P.name LIKE '%s' OR P.name LIKE '%S'
ORDER BY P.name
;