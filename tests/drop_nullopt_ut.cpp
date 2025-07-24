#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace pipeline;

TEST(DropNulloptTest, DropNullopt) {
    std::vector<std::optional<int>> input = {1, std::nullopt, 3, std::nullopt, 5};
    auto result = AsDataFlow(input) | DropNullopt() | AsVector();
    ASSERT_THAT(result, testing::ElementsAre(1, 3, 5));
}


