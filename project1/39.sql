SELECT P.name
FROM Trainer AS T, Gym AS G, CatchedPokemon AS CP, Pokemon AS P
WHERE T.id = G.leader_id AND G.leader_id = CP.owner_id AND CP.pid = P.id
	AND G.city = 'Rainbow City'
ORDER BY P.name
;