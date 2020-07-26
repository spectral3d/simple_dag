#ifndef INCLUDED_S3D_DAG_H
#define INCLUDED_S3D_DAG_H

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include <vector>

namespace s3d_graph
{
    // An edge in a directed graph.  It points from src to dst.
    template<typename NodeID>
    class directed_edge
    {
    public:
        using node_id_type  = NodeID;

        directed_edge()
            : m_src(0)
            , m_dst(0)
        {
        }

        explicit directed_edge(node_id_type src, node_id_type dst)
            : m_src(src)
            , m_dst(dst)
        {
        }

        const node_id_type get_src() const { return m_src; }
        const node_id_type get_dst() const { return m_dst; }
    private:
        node_id_type m_src, m_dst;
    };

    template<typename NodeID>
    class dag
    {
    public:
        using node_id_type      = NodeID;
        using node_id_vector    = std::vector<node_id_type>;
        using edge_type         = directed_edge<node_id_type>;
        using edge_vector       = std::vector<edge_type>;

        // Construct a DAG given a collection of edges.
        //
        // Assumption is that edges order "src" nodes before "dst" nodes.
        //
        // vectors are used rather than sets to keep things nicely
        // laid out in memory.
        template<typename EdgeIterator>
        dag(    EdgeIterator edge_begin,
                EdgeIterator edge_end,
                std::enable_if_t<
                    std::is_same<
                        edge_type,
                        typename std::iterator_traits<EdgeIterator>::value_type>::value,
                    int> = 0)
            : m_valid(false)
        {
            node_id_vector tmp;
            build(edge_begin, edge_end, tmp.begin(), tmp.end());
        }

        // Construct a DAG given a collection of edges and nodes.
        // Nodes that are referenced by edges will be added so the
        // node iterators may be used to reference orphan nodes not
        // referenced by any edges.
        //
        // Assumption is that edges order "src" nodes before "dst" nodes.
        //
        // vectors are used rather than sets to keep things nicely
        // laid out in memory.
        template<typename EdgeIterator, typename NodeIterator>
        dag(
                EdgeIterator edge_begin,
                EdgeIterator edge_end,
                NodeIterator node_begin,
                NodeIterator node_end,
                std::enable_if_t<
                    std::is_same<
                        edge_type,
                        typename std::iterator_traits<EdgeIterator>::value_type>::value &&
                    std::is_same<
                        node_id_type,
                        typename std::iterator_traits<NodeIterator>::value_type>::value,
                    int> = 0)
            : m_valid(false)
        {
            build(edge_begin, edge_end, node_begin, node_end);
        }

        // Is this in a valid state.  Will return false if the input was not
        // a DAG.
        bool get_valid() const { return m_valid; }

        // Get all nodes, sorted by id.
        node_id_vector const &get_all_nodes() const
        {
             return m_all_nodes;
        }

        // Get nodes in topological order.  Will be empty if this is not a DAG.
        node_id_vector const &get_sorted_nodes() const
        {
             return m_sorted_nodes;
        }

        // Get edges sorted by src id.
        edge_vector const &get_edges_by_src() const
        {
            return m_edges_by_src;
        }

        // Get edges sorted by dst id.
        edge_vector const &get_edges_by_dst() const
        {
            return m_edges_by_dst;
        }

    private:
        // Construction helpers.

        // Construct a DAG given a collection of edges and nodes.
        template<typename EdgeIterator, typename NodeIterator>
        void build(
                EdgeIterator edge_begin,
                EdgeIterator edge_end,
                NodeIterator node_begin,
                NodeIterator node_end)
        {
            // Set up edge vectors.
            m_edges_by_src.insert(m_edges_by_src.end(), edge_begin, edge_end);

            std::sort(
                m_edges_by_src.begin(),
                m_edges_by_src.end(),
                [](auto &a, auto &b){return a.get_src() < b.get_src();});

            m_edges_by_dst.insert(m_edges_by_dst.end(), edge_begin, edge_end);

            std::sort(
                m_edges_by_dst.begin(),
                m_edges_by_dst.end(),
                [](auto &a, auto &b){return a.get_dst() < b.get_dst();});

            // gather nodes.
            {
                node_id_vector all_nodes(node_begin, node_end);

                std::for_each(
                    m_edges_by_src.begin(),
                    m_edges_by_src.end(),
                    [&all_nodes](auto &edge)
                    {
                        all_nodes.push_back(edge.get_src());
                        all_nodes.push_back(edge.get_dst());
                    });

                // Sort and apply uniqueness criterion
                std::sort(all_nodes.begin(), all_nodes.end());

                all_nodes.erase(
                    std::unique(all_nodes.begin(), all_nodes.end()),
                    all_nodes.end());

                m_all_nodes = all_nodes;
            }

            topological_sort();
        }

        // Sort into topological order if possible.
        void topological_sort()
        {
            edge_vector remaining_edges(m_edges_by_dst);
            node_id_vector remaining_nodes(m_all_nodes);
            node_id_vector to_go;

            m_sorted_nodes.clear();

            // Empty graph is valid.
            m_valid = true;

            while(m_valid && !remaining_nodes.empty())
            {
                to_go.clear();
                m_valid = false;

                for(auto &node : remaining_nodes)
                {
                    // Find nodes
                    auto edge_it = std::lower_bound(
                        remaining_edges.begin(),
                        remaining_edges.end(),
                        node,
                        [](auto &e, auto &n) { return e.get_dst() < n; });

                    if(!((edge_it != remaining_edges.end()) &&
                        (edge_it->get_dst() == node)))
                    {
                        // push this node to the output and tag it as to be
                        // removed.
                        m_sorted_nodes.push_back(node);
                        to_go.push_back(node);
                        m_valid = true;
                    }
                }

                if(!to_go.empty())
                {
                    // First remove processed nodes.
                    remaining_nodes.erase(
                        std::remove_if(
                            remaining_nodes.begin(),
                            remaining_nodes.end(),
                            [&to_go](auto &n)
                            {
                                return std::binary_search(
                                    to_go.begin(),
                                    to_go.end(),
                                    n);
                            }),
                        remaining_nodes.end());

                    // Now remove edges coming from these nodes.
                    remaining_edges.erase(
                        std::remove_if(
                            remaining_edges.begin(),
                            remaining_edges.end(),
                            [&to_go](auto &e)
                            {
                                return std::binary_search(
                                    to_go.begin(),
                                    to_go.end(),
                                    e.get_src());
                            }),
                        remaining_edges.end());
                }
            }

            if(!m_valid)
            {
                m_sorted_nodes.clear();
            }
        }

        // true if we have a
        bool            m_valid;

        // We keep two copies of the edges for efficient searches up and down
        // the graph.
        edge_vector     m_edges_by_src,
                        m_edges_by_dst;

        node_id_vector  m_all_nodes,
                        m_sorted_nodes;
    };

    // External functions.

    // Given a dag and a node, what nodes have edges leading to this node?
    // output will be sorted by node id.
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
            }

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
