#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace pipeline;

TEST(ReadTest, ByNewLine) {
    std::vector<std::stringstream> files(2);
    files[0] << "1\n2\n3\n4\n5";
    files[1] << "6\n7\n8\n9\n10";
    auto result = AsDataFlow(files) | Split("\n") | AsVector();
    ASSERT_THAT(result, testing::ElementsAre("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
}

TEST(ReadTest, BySpace) {
    std::vector<std::stringstream> files(2);
    files[0] << "1 2 3 4 5";
    files[1] << "6 7 8 9 10";
    auto result = AsDataFlow(files) | Split(" ") | AsVector();
    ASSERT_THAT(result, testing::ElementsAre("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
}


