/**
 * @file   map_sim.cpp
 * @author Changsheng Jiang <jiangzuoyan@gmail.com>
 * @date   Fri Oct 21 17:07:48 2011
 *
 * @brief map similarity
 *
 *
 */
#include "douban/map_utility.hpp"
#include <boost/python.hpp>
#include <boost/python/stl_iterator.hpp>
#include <douban/fmtstr.hpp>

namespace bpy = boost::python;

namespace  {

template <class K, class Func>
void input_map(
    K /**/,
    bpy::object &A, Func func) {
    bpy::stl_input_iterator<bpy::object> first(A), last;
    while (first != last) {
      K k = bpy::extract<K>(*first);
      bpy::object v = A[*first];
      func(k, v);
      ++first;
    }
}

} // namespace anonymous

// return (A * B' ktop by row)
bpy::object map_sim(
    bpy::object R, bpy::object A, bpy::object B, size_t ktop) {
  douban::map_type<size_t, size_t, double>::type a;
  douban::map_type<size_t, size_t, double>::type b;
  douban::map_type<size_t, std::vector< std::pair<size_t, double> > >::type sims;

  auto input_map2 = [&](
      bpy::object &A,
      douban::map_type<size_t, size_t, double>::type &a) {
    input_map(
        (size_t)0, A,
        [&](size_t k, bpy::object v) {
          auto &d = a[k];
          input_map(k, v, [&](size_t i, bpy::object vv) {
              double x = bpy::extract<double>(vv);
            d[i] = x;
            });
        });
  };
  input_map2(A, a);
  input_map2(B, b);
  b = douban::map_reverse2(b);

  auto sim_cmp = [](const std::pair<size_t, double> &a,
                    const std::pair<size_t, double> &b) {
    return douban::cmp(a.second, b.second);
  };

  auto insert_sim = [&](
      size_t f, std::vector<std::pair<size_t, double> > &vsim, size_t t, double s) {
    if (f == t) return; // ignore self sim.
    auto elm = std::make_pair(t, s);
    if (vsim.size() < (size_t)ktop) {
      vsim.push_back(elm);
      if (vsim.size() == (size_t)ktop) {
        douban::heapify(vsim.begin(), vsim.end(), sim_cmp);
      }
    } else {
      if (sim_cmp(vsim[0], elm) < 0) {
        vsim[0] = elm;
        douban::heap_shiftdown(vsim.begin(), vsim.end(), vsim.begin(), sim_cmp);
      }
    }
  };

  input_map(
      (size_t)0, R,
      [&](size_t n, bpy::object v) {
        bpy::stl_input_iterator<bpy::object> first(v), last;
        auto &d = sims[n];
        while (first != last) {
          bpy::object elm = *first;
          size_t j = bpy::extract<size_t>(elm[0]);
          double s = bpy::extract<double>(elm[1]);
          insert_sim(n, d, j, s);
          ++first;
        }
      });

  BOOST_FOREACH (auto &irow, a) {
    auto i = irow.first;
    douban::map_type<size_t, double>::type isim;
    BOOST_FOREACH (auto &kv, irow.second) {
      auto k = kv.first;
      auto av = kv.second;
      auto it = b.find(k);
      if (it == b.end()) continue;
      BOOST_FOREACH (auto &jv, it->second) {
        auto j = jv.first;
        auto bv = jv.second;
        douban::set_default(isim, j, 0.) += av * bv;
      }
    }
    auto &vsim = sims[i];
    BOOST_FOREACH(auto &js, isim) {
      insert_sim(i, vsim, js.first, js.second);
    }
  }

  bpy::dict ret;
  BOOST_FOREACH (auto &ns, sims) {
    auto n = ns.first;
    bpy::list l;
    BOOST_FOREACH (auto &js, ns.second) {
      l.append(bpy::make_tuple(js.first, js.second));
    }
    ret[n] = l;
  }
  // douban::fmterrlnt("A=(#%s, nnz=%s), B=(#%s, nnz=%s), ktop=%s",
  //                   a.size(), douban::map_size2(a),
  //                   b.size(), douban::map_size2(b),
  //                   ktop);
  return ret;
}

BOOST_PYTHON_MODULE(map_sim) {
  bpy::def("map_sim", &map_sim, "map_sim(A, B, ktop)");
};



