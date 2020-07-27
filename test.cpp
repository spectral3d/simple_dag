#include "dag.h"
#include "algorithms.h"
#include <cstdio>

int main(int, char **)
{
    using namespace s3d_graph;
    using dag_type  = dag<uint32_t>;
    using edge_type = typename dag_type::edge_type;

    std::vector<edge_type> edges;
    std::vector<uint32_t> extra_nodes;

    printf("test with simple dag:\n");
    printf("    0->1->2-\\\n");
    printf("    \\->3---->4\n");

    edges.emplace_back(0, 1);
    edges.emplace_back(1, 2);
    edges.emplace_back(0, 3);
    edges.emplace_back(3, 4);
    edges.emplace_back(2, 4);

    dag_type graph(edges.begin(), edges.end());

    {
        printf("\ntopological order (expect 0, 1, 3, 2, 4): \n");
        for(auto &n : graph.get_sorted_nodes())
        {
            printf("%i\n", n);
        }
    }

    {
        printf("\nedges by src\n");

        for(auto &e : graph.get_edges_by_src())
        {
            printf("%i, %i\n", e.get_src(), e.get_dst());
        }
    }

    {
        printf("\nedges by dst\n");

        for(auto &e : graph.get_edges_by_dst())
        {
            printf("%i, %i\n", e.get_src(), e.get_dst());
        }
    }
    
    {
        std::vector<uint32_t> before;
        find_all_before(graph, 2u, before);

        printf("\nnodes before 2 (expect 0, 1) : \n");
        for(auto &n : before)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> before;
        find_all_before(graph, 3u, before);

        printf("\nnodes before 3 (expect 0) : \n");
        for(auto &n : before)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> after;
        find_all_after(graph, 2u, after);

        printf("\nnodes after 2 (expect 4) : \n");
        for(auto &n : after)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> siblings;
        find_all_siblings(graph, 2u, siblings);

        printf("\nsiblings of 2 (expect 3) : \n");
        for(auto &n : siblings)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> siblings;
        find_all_siblings(graph, 3u, siblings);

        printf("\nsiblings of 3 (expect 1, 2) : \n");
        for(auto &n : siblings)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> done = {0};
        std::vector<uint32_t> next;
        find_current_tasks(graph, done, next);

        printf("\nrun tasks after 0 (expect 1, 3) : \n");

        for(auto &n : next)
        {
            printf("%i\n", n);
        }
    }

    {
        std::vector<uint32_t> done = {0, 1};
        std::vector<uint32_t> next;
        find_current_tasks(graph, done, next);

        printf("\nrun tasks after 0, 1 (expect 2, 3) : \n");

        for(auto &n : next)
        {
            printf("%i\n", n);
        }
    }

    return 0;
}
