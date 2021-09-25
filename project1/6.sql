SELECT L.NAME, AVG(CP.level)
FROM CatchedPokemon AS CP, (
    SELECT T.id AS 'ID' , T.name AS 'NAME'
    FROM Trainer AS T, Gym AS G
    WHERE T.id = G.leader_id
  ) AS L
WHERE CP.owner_id = L.ID
GROUP BY L.ID
ORDER BY L.NAME
;