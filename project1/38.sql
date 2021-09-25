SELECT T.name
FROM Gym AS G, Trainer AS T
WHERE G.leader_id = T.id AND G.city = 'Brown City'
;