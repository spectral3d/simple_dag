#ifndef INCLUDED_S3D_DAG_ALGORITHMS_H
#define INCLUDED_S3D_DAG_ALGORITHMS_H

#include "dag.h"

namespace s3d_graph
{
    // Algorithms that operate on directed graphs. 

    // Given a dag and a node, what nodes have edges leading directly to this
    // node?
    // output will be sorted by node id.
    template<typename T>
    bool find_before(
            dag<T> const &graph,
            T node_id,
            typename dag<T>::node_id_vector &out)
    {
        using node_id_vector    = typename dag<T>::node_id_vector;

        out.clear();

        // Ensure the graph is valid.  We don't actually care if it
        // contains this node.   
        if(graph.get_valid())
        {
            auto &edges = graph.get_edges_by_dst();

            auto edge_it = std::lower_bound(
                edges.begin(),
                edges.end(),
                node_id,
                [](auto &e, auto &n) { return e.get_dst() < n; });

            while ((edge_it != edges.end()) &&
                   (edge_it->get_dst() == node_id))
            {
                out.emplace_back(edge_it->get_src());
                ++edge_it;
            }

            std::sort(out.begin(), out.end());
            out.erase(std::unique(out.begin(), out.end()), out.end());
            return true;
        }
        else
        {
            return false;
        }
    }

    // Given a dag and a node, what nodes have edges leading directly from this
    // node?
    // output will be sorted by node id.
    template<typename T>
    bool find_after(
            dag<T> const &graph,
            T node_id,
            typename dag<T>::node_id_vector &out)
    {
        using node_id_vector    = typename dag<T>::node_id_vector;

        out.clear();

        // Ensure the graph is valid.  We don't actually care if it
        // contains this node.   
        if(graph.get_valid())
        {
            auto &edges = graph.get_edges_by_src();

            auto edge_it = std::lower_bound(
                edges.begin(),
                edges.end(),
                node_id,
                [](auto &e, auto &n) { return e.get_src() < n; });

            while ((edge_it != edges.end()) &&
                   (edge_it->get_src() == node_id))
            {
                out.emplace_back(edge_it->get_dst());
                ++edge_it;
            }

            std::sort(out.begin(), out.end());
            out.erase(std::unique(out.begin(), out.end()), out.end());
            return true;
        }
        else
        {
            return false;
        }
    }
 
    template<typename T>
    bool find_all_before(
            dag<T> const &graph,
            T node_id,
            typename dag<T>::node_id_vector &out)
    {
        using node_id_vector    = typename dag<T>::node_id_vector;

        out.clear();

        // Ensure the graph is valid.  We don't actually care if it
        // contains this node.   
        if(graph.get_valid())
        {
            node_id_vector to_process;
            auto &edges = graph.get_edges_by_dst();

            to_process.push_back(node_id);

            // Depth first back up the graph.
            while(!to_process.empty())
            {
                auto cur_id = to_process.back();
                to_process.pop_back();

                // Find all edges that point to this node.
                auto edge_it = std::lower_bound(
                    edges.begin(),
                    edges.end(),
                    cur_id,
                    [](auto &e, auto &n) { return e.get_dst() < n; });

                while((edge_it != edges.end()) && 
                      (edge_it->get_dst() == cur_id))
                {
                    // Find insertion point in output.
                    auto ins_it = std::lower_bound(
                        out.begin(),
                        out.end(),
                        edge_it->get_src());

                    if((ins_it == out.end()) ||
                       (edge_it->get_src() != *ins_it))
                    {
                        out.insert(ins_it, edge_it->get_src());
                        to_process.push_back(edge_it->get_src());
                    }

                    ++edge_it;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }
 
    // Given a dag and a node, what nodes can be reached from this node?
    // output will be sorted by node id.
    template<typename T>
    bool find_all_after(
            dag<T> const &graph,
            T node_id,
            typename dag<T>::node_id_vector &out)
    {
        using node_id_vector    = typename dag<T>::node_id_vector;

        out.clear();

        // Ensure the graph is valid.  We don't actually care if it
        // contains this node.   
        if(graph.get_valid())
        {
            node_id_vector to_process;
            auto &edges = graph.get_edges_by_src();

            to_process.push_back(node_id);

            // Depth first down the graph.
            while(!to_process.empty())
            {
                auto cur_id = to_process.back();
                to_process.pop_back();

                // Find all edges that point from this node.
                auto edge_it = std::lower_bound(
                    edges.begin(),
                    edges.end(),
                    cur_id,
                    [](auto &e, auto &n) { return e.get_src() < n; });

                while((edge_it != edges.end()) && 
                      (edge_it->get_src() == cur_id))
                {
                    // Find insertion point in output.
                    auto ins_it = std::lower_bound(
                        out.begin(),
                        out.end(),
                        edge_it->get_dst());

                    if((ins_it == out.end()) ||
                       (edge_it->get_dst() != *ins_it))
                    {
                        out.insert(ins_it, edge_it->get_dst());
                        to_process.push_back(edge_it->get_dst());
                    }

                    ++edge_it;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    } 

    // Given a DAG used in a scheduler, what could potentially run at the same 
    // time as this?
    // output will be sorted by node id.
    template<typename T>
    bool find_all_siblings(
            dag<T> const &graph,
            T node_id,
            typename dag<T>::node_id_vector &out)
    {
        using node_id_vector = typename dag<T>::node_id_vector;

        out.clear();

        // Ensure graph is valid and contains the node.
        if(graph.get_valid() &&
           std::binary_search(graph.get_all_nodes().begin(),
                              graph.get_all_nodes().end(),
                              node_id))
        {
            // Find everything before and after this node.
            node_id_vector before, after;

            find_all_before(graph, node_id, before);
            find_all_after(graph, node_id, after);

            // fast out for case when there are no siblings.
            if((before.size() + after.size() + 1) < 
               graph.get_all_nodes().size())
            {
                out = graph.get_all_nodes();

                // Remove everything in the before & after vectors, along
                // with the input.
                out.erase(
                    std::remove_if(
                        out.begin(),
                        out.end(),
                        [&before, &after, node_id](auto n)
                        {
                            return 
                                (n == node_id) ||
                                std::binary_search(
                                    before.begin(), before.end(), n) ||
                                std::binary_search(
                                    after.begin(), after.end(), n);
                        }),
                        out.end());
            }
            return true;
        }
        else
        {
            return false;
        }
    }


    // This assumes you are using the DAG for some sort of scheduling operation
    //
    // Finds tasks that could be scheduled now given a set of completed tasks.
    // "done" vector must be sorted, as it is being used as a set.
    template<typename T>
    bool find_current_tasks(
            dag<T> const &graph,
            typename dag<T>::node_id_vector const &done,
            typename dag<T>::node_id_vector &out)
    {
        out.clear();

        if(graph.get_valid())
        {
            out = graph.get_all_nodes();
            auto edges = graph.get_edges_by_dst();

            // erase all nodes in the "done" set.
            out.erase(
                std::remove_if(
                    out.begin(),
                    out.end(),
                    [&done](auto &n)
                    {
                        return std::binary_search(
                            done.begin(),
                            done.end(),
                            n);
                    }),
                out.end());

            // erase all edges coming from the done set.
            edges.erase(
                std::remove_if(
                    edges.begin(),
                    edges.end(),
                    [&done](auto &e)
                    {
                        return std::binary_search(
                            done.begin(),
                            done.end(),
                            e.get_src());
                    }),
                edges.end());

            // erase all nodes pointed to by remaining edges.
            out.erase(
                std::remove_if(
                    out.begin(),
                    out.end(),
                    [&edges](auto &n)
                    {
                        auto e_it = std::lower_bound(
                            edges.begin(),
                            edges.end(),
                            n,
                            [](auto &e, auto &n)
                            {
                                return e.get_dst() < n;
                            });

                        return  (e_it != edges.end()) &&
                                (e_it->get_dst() == n);
                    }),
                out.end());                     

            return true;
        }
        else
        {
            return false;
        }
    }
}

#endif
