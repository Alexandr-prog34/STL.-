#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>

using namespace pipeline;


TEST(ReadTest, ByTab) {
    std::vector<std::stringstream> files(2);
    files[0] << "1\t2\t3\t4\t5";
    files[1] << "6\t7\t8\t9\t10";
    auto result = AsDataFlow(files) | Split("\t") | AsVector();
    ASSERT_THAT(result, testing::ElementsAre("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
}


TEST(FilterTest, FilterOdd) {
    std::vector<int> input = {1, 2, 3, 4, 5};
    auto result = AsDataFlow(input) | Filter([](int x) { return x % 2!= 0; }) | AsVector();
    ASSERT_THAT(result, testing::ElementsAre(1, 3, 5));
}


TEST(FilterTest, FilterCustom) {
    std::vector<int> input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    auto result = AsDataFlow(input) | Filter([](int x) { return x % 3 != 0; }) | AsVector();
    ASSERT_THAT(result, testing::ElementsAre(1, 2, 4, 5, 7, 8, 10, 11));
}


TEST(FlowConvertionsTest, AsVectorones) {
    std::vector<int> input = {0};
    auto result = AsDataFlow(input) | AsVector();
    ASSERT_THAT(result, testing::ElementsAreArray(std::vector<int>{0}));
}


TEST(ReadTest, BySpace2) {
    std::vector<std::stringstream> files(2);
    files[0] << "12 13 44 1873";
    files[1] << "5 4 3 2 1";
    auto result = AsDataFlow(files) | Split(" ") | AsVector();
    ASSERT_THAT(result, testing::ElementsAre("12","13","44","1873","5","4","3","2","1"));
}



TEST(DirOutTest, Out) {
    std::vector<int> input = {10, 20, 30};
    std::stringstream ss;
    
    auto flow = AsDataFlow(input) | Out(ss);
    
    ASSERT_EQ(ss.str(), "10 20 30 ");
}


TEST(RandomTest, TransformAndFilter) {
    std::vector<int> input = {1, 2, 3, 4, 5, 6};
    auto result = AsDataFlow(input)
                | Transform([](int x) { return x * 3; })
                | Filter([](int x) { return x % 2 == 0; })
                | AsVector();
    ASSERT_THAT(result, ::testing::ElementsAre(6, 12, 18));
}

TEST(RandomTest, SplitAndTransform) {
    std::vector<std::stringstream> files(1);
    files[0] << "10,20,30,40";
    auto result = AsDataFlow(files)
                | Split(",")
                | Transform([](const std::string& s) { return std::stoi(s) + 1; })
                | AsVector();
    ASSERT_THAT(result, ::testing::ElementsAre(11, 21, 31, 41));
}

TEST(RandomTest, OutAndFilter) {
    std::vector<std::string> input = {"apple", "banana", "cherry", "date"};
    auto filtered = AsDataFlow(input)
                  | Filter([](const std::string& s) { return s.size() > 5; });
    std::stringstream ss;
    filtered | Out(ss);
    ASSERT_EQ(ss.str(), "banana cherry ");
}


TEST(KVTest, EqualityOperator) {
    KV<int, std::string> kv1{1, "value1"};
    KV<int, std::string> kv2{1, "value1"};
    KV<int, std::string> kv3{2, "value2"};

    EXPECT_TRUE(kv1 == kv2);
    EXPECT_FALSE(kv1 == kv3);
}
