# Python3 Program for Floyd Warshall Algorithm

# Solves all pair shortest path
# via Floyd Warshall Algorithm

class FloydWarshall():

    def __init__(self, graph):
        self.__distances = self.__run(graph)

    def get_distances(self):
        return self.__distances

    def __run(self, graph):
        """ dist[][] will be the output
           matrix that will finally
            have the shortest distances
            between every pair of vertices """

        """ initializing the solution matrix 
        same as input graph matrix
        OR we can say that the initial 
        values of shortest distances
        are based on shortest paths considering no 
        intermediate vertices """
        dist = list(map(lambda i: list(map(lambda j: j, i)), graph))

        """ Add all vertices one by one 
        to the set of intermediate
         vertices.
         ---> Before start of an iteration, 
         we have shortest distances
         between all pairs of vertices 
         such that the shortest
         distances consider only the 
         vertices in the set 
        {0, 1, 2, .. k-1} as intermediate vertices.
          ----> After the end of a 
          iteration, vertex no. k is
         added to the set of intermediate 
         vertices and the 
        set becomes {0, 1, 2, .. k}
        """
        for k in range(V):
            # pick all vertices as source one by one
            for i in range(V):
                # Pick all vertices as destination for the
                # above picked source
                for j in range(V):
                    # If vertex k is on the shortest path from
                    # i to j, then update the value of dist[i][j]
                    dist[i][j] = min(dist[i][j], dist[i][k] + dist[k][j])
        return dist