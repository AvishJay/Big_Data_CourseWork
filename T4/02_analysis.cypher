
// ---------------------------------------------------------------------------
// Q0 
MATCH (p:Patent)<-[:CITES]-()
RETURN p.id AS patent, count(*) AS incoming
ORDER BY incoming DESC LIMIT 5;

// ---------------------------------------------------------------------------
// Q1 - Direct neighbour layers of a specific target reference item.

PROFILE
MATCH (target:Patent {id: 3338819})
OPTIONAL MATCH (target)-[:CITES]->(cited:Patent)
OPTIONAL MATCH (target)<-[:CITES]-(citing:Patent)
RETURN target.id                    AS patent,
       collect(DISTINCT cited.id)  AS references_out,
       collect(DISTINCT citing.id) AS cited_by;

// ---------------------------------------------------------------------------
// Q2 - Degree centrality: top-tier patents by volume of INCOMING citation

PROFILE
MATCH (p:Patent)<-[c:CITES]-()
RETURN p.id AS patent, count(c) AS in_degree
ORDER BY in_degree DESC
LIMIT 10;

// ---------------------------------------------------------------------------
// Q3 - Shortest path between two distant node points, mapping hidden

PROFILE
MATCH (a:Patent {id: 3338819}), (b:Patent {id: 3717571})
MATCH path = shortestPath((a)-[:CITES*..15]-(b))
RETURN [n IN nodes(path) | n.id] AS linkage_chain,
       length(path)              AS hops;


