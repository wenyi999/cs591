in task 2, when track_prov=false, the most time-consuming operator is the join operator, specifically is the initial part of the join operator (right join), 
the second most time-consuming operator is the select operator.
When the track_prov=true, all operators have an overhead, but the order by operator has the most increase in the overhead.
in task 3, the most time-consuming lineage is the scan lineage,  
the second most time-consuming operator is another scan lineage.