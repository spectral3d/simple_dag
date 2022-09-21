#ifndef INCLUDED_S3D_DAG_H
#define INCLUDED_S3D_DAG_H

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include <vector>
#include <queue>

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
            m_sorted_nodes.clear();

            // Empty graph is valid.
            m_valid = true;

            using count_record = std::pair<node_id_type, size_t>;

            std::vector<count_record> incoming_counts;

            incoming_counts.reserve(m_all_nodes.size());

            std::transform(
                m_all_nodes.begin(),
                m_all_nodes.end(),
                std::back_inserter(incoming_counts),
                [](node_id_type const &id)->count_record
                { return {id, size_t(0)}; });

            // Initialise count of incoming edges.
            std::for_each(
                m_edges_by_dst.begin(),
                m_edges_by_dst.end(),
                [&incoming_counts](auto &edge)
                {
                    auto it = std::lower_bound(
                        incoming_counts.begin(),
                        incoming_counts.end(),
                        edge.get_dst(),
                        [](auto &count, auto &id)
                        {
                            return count.first < id;
                        });

                    ++it->second;
                });
                
            std::queue<node_id_type> to_process;

            for(auto &count : incoming_counts)
            {
                // Find "root" vertices of graph.
                if(count.second == 0)
                {
                    m_sorted_nodes.emplace_back(count.first);

                    // Find edges from these vertices and add their destinations
                    // to output.
                    auto begin_it = std::lower_bound(
                        m_edges_by_src.begin(),
                        m_edges_by_src.end(),
                        count.first,
                        [](auto &edge, auto &id)
                        {
                            return edge.get_src() < id;
                        });

                    auto end_it = std::upper_bound(
                        m_edges_by_src.begin(),
                        m_edges_by_src.end(),
                        count.first,
                        [](auto &id, auto &edge)
                        {
                            return id < edge.get_src();
                        });

                    std::for_each(
                        begin_it,
                        end_it,
                        [&to_process](auto &edge)
                        {
                            to_process.push(edge.get_dst());
                        });
                }
            }

            while(!to_process.empty())
            {
                auto next_vertex = to_process.front();

                to_process.pop();

                auto count_it = std::lower_bound(
                    incoming_counts.begin(),
                    incoming_counts.end(),
                    next_vertex,
                    [](auto &count, auto &id)
                    {
                        return count.first < id;
                    });

                auto &count = *count_it;

                if(--count.second == 0)
                {
                    m_sorted_nodes.emplace_back(count.first);

                    // Find edges from these vertices and add their destinations
                    // to output.
                    auto begin_it = std::lower_bound(
                        m_edges_by_src.begin(),
                        m_edges_by_src.end(),
                        count.first,
                        [](auto &edge, auto &id)
                        {
                            return edge.get_src() < id;
                        });

                    auto end_it = std::upper_bound(
                        m_edges_by_src.begin(),
                        m_edges_by_src.end(),
                        count.first,
                        [](auto &id, auto &edge)
                        {
                            return id < edge.get_src();
                        });

                    std::for_each(
                        begin_it,
                        end_it,
                        [&to_process](auto &edge)
                        {
                            to_process.push(edge.get_dst());
                        });
                }
            }

            m_valid = (m_sorted_nodes.size() == m_all_nodes.size());

            if(!m_valid)
            {
                m_sorted_nodes.clear();
            }
        }

        // true if we have a DAG.
        bool            m_valid;

        // We keep two copies of the edges for efficient searches up and down
        // the graph.
        edge_vector     m_edges_by_src,
                        m_edges_by_dst;

        node_id_vector  m_all_nodes,
                        m_sorted_nodes;
    };
}

#endif
