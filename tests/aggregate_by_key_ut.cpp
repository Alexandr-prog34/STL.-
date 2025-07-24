#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace pipeline;

struct Employee {
    uint64_t department_id;
    std::string name;

    bool operator==(const Employee& other) const = default;
};

TEST(AggregateByKeyTest, CountingAggregatedValues) {
    std::vector<std::string> input = {"name4", "name0", "name1", "name0", "name2", "name0", "name1"};

    auto result =
        AsDataFlow(input)
            | AggregateByKey(
                std::size_t{0},
                [](const std::string&, std::size_t& accumulated) { ++accumulated; },
                [](const std::string& token) { return token; }
            )
            | AsVector();

    ASSERT_THAT(
        result,
        ::testing::ElementsAre(
            std::make_pair("name4", 1),
            std::make_pair("name0", 3),
            std::make_pair("name1", 2),
            std::make_pair("name2", 1)
        )
    );
}

TEST(AggregateByKeyTest, AggregatingWithSeveralOutputsForEachKey) {
    std::vector<Employee> employees = {
        {3, "name1"},
        {1, "name1"},
        {1, "name2"},
        {2, "name1"},
        {2, "name2"},
        {2, "name3"}
    };

    auto result =
        AsDataFlow(employees)
            | AggregateByKey(
                std::vector<Employee>{},
                [](const Employee& employee, std::vector<Employee>& accumulated) {
                    if (accumulated.size() == 2) {
                        return;
                    }
                    accumulated.push_back(employee);
                },
                [](const Employee& employee) { return employee.department_id; }
            )
            | AsVector();

    ASSERT_THAT(
        result,
        ::testing::ElementsAre(
            std::make_pair(3, std::vector<Employee>{Employee{3, "name1"}}),
            std::make_pair(1, std::vector<Employee>{Employee{1, "name1"}, Employee{1, "name2"}}),
            std::make_pair(2, std::vector<Employee>{Employee{2, "name1"}, Employee{2, "name2"}})
        )
    );
}



