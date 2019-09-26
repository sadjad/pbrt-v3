#ifndef PBRT_UTIL_RANDOM_H
#define PBRT_UTIL_RANDOM_H

#include <iterator>
#include <random>
#include <stdexcept>
#include <iostream>
namespace pbrt {
namespace random {

template <typename Iter, typename RandomGenerator>
Iter sample(Iter start, Iter end, RandomGenerator& g) {
    if (start == end) {
        std::cout << "AHHHHHHHHH THE LIST WE ARE TRYING TO GET A SAMPLE FROM IS EMPTY" << std::endl;
        throw std::runtime_error("the list is empty");
    }
    
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

template <typename Iter>
Iter sample(Iter start, Iter end) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return sample(start, end, gen);
}

}  // namespace random
}  // namespace pbrt

#endif /* PBRT_UTIL_RANDOM_H */
