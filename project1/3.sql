SELECT T.name
FROM Trainer T
WHERE NOT T.id IN (
  SELECT G.leader_id
  FROM Gym AS G
  )
ORDER BY T.name
;