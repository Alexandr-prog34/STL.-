#pragma once

#include <vector>
#include <unordered_map>
#include <type_traits>
#include <utility>
#include <iterator>
#include <optional>
#include <filesystem>
#include <fstream>
#include <ostream>


namespace pipeline {

template <typename K, typename V>
struct KV {
    K key;
    V value;

    bool operator==(const KV&) const = default;
};

template <typename L, typename R>
struct JoinResult {
    L left;
    std::optional<R> right;

    bool operator==(const JoinResult&) const = default;
};
    

template <typename Flow, typename Adapter>
auto operator|(Flow&& flow, Adapter&& adapter)
{
    return std::forward<Adapter>(adapter)(std::forward<Flow>(flow));
}


template <typename Container>
class DataFlowContainer {
public:
    using value_type = typename Container::value_type;
    using const_iterator = typename Container::const_iterator;
    explicit DataFlowContainer(Container& cont)
        : container_(&cont)
    {}

    auto begin() const { return container_->begin(); }
    auto end() const { return container_->end();   }

private:
    Container* container_;
};


template <typename Container>
auto AsDataFlow(Container& cont)
{
    return DataFlowContainer<Container>(cont);
}


template <typename ValueInit, typename Aggregator, typename KeyGetter>
class AggregateByKeyAdapter
{
public:
    AggregateByKeyAdapter(ValueInit valueInit, Aggregator aggregator, KeyGetter keyGetter)
        : valueInit_(std::move(valueInit))
        , aggregator_(std::move(aggregator))
        , keyGetter_(std::move(keyGetter))
    {}

    template <typename Flow>
    auto operator()(Flow&& flow)
    {
        using ItemType = decltype(*std::begin(flow));
        using KeyType  = std::decay_t<decltype(keyGetter_(std::declval<ItemType>()))>;
        using ValueType = std::decay_t<ValueInit>;

        std::vector<std::pair<KeyType, ValueType>> result;

        std::unordered_map<KeyType, std::size_t> indexMap;

        auto dist = std::distance(std::begin(flow), std::end(flow));
        if (dist > 0) {
            result.reserve(static_cast<std::size_t>(dist));
            indexMap.reserve(static_cast<std::size_t>(dist));
        }

        for (auto&& item : flow) {
            KeyType key = keyGetter_(item);

            auto it = indexMap.find(key);
            if (it == indexMap.end()) {
                indexMap[key] = result.size();
                result.emplace_back(std::move(key), ValueType(valueInit_));
                aggregator_(item, result.back().second);
            } else {
                aggregator_(item, result[it->second].second);
            }
        }

        struct OutputFlow {
            explicit OutputFlow(std::vector<std::pair<KeyType, ValueType>> data)
                : data_(std::move(data))
            {}

            auto begin() { return data_.begin(); }
            auto end()   { return data_.end();   }

            auto begin() const { return data_.begin(); }
            auto end()   const { return data_.end();   }

            std::vector<std::pair<KeyType, ValueType>> data_;
        };

        return OutputFlow(std::move(result));
    }


private:
    ValueInit valueInit_;
    Aggregator aggregator_;
    KeyGetter keyGetter_;
};


template <typename ValueInit, typename Aggregator, typename KeyGetter>
auto AggregateByKey(ValueInit valueInit, Aggregator aggregator, KeyGetter keyGetter)
{
    return AggregateByKeyAdapter<ValueInit, Aggregator, KeyGetter>(
        std::move(valueInit),
        std::move(aggregator),
        std::move(keyGetter)
    );
}


class AsVectorAdapter {
public:
    template <typename Flow>
    auto operator()(Flow&& flow)
    {
        using ItemType = std::decay_t<decltype(*std::begin(flow))>;
        std::vector<ItemType> output;
        for (auto&& item : flow) {
            output.push_back(item);
        }
        return output;
    }
};

inline auto AsVector()
{
    return AsVectorAdapter{};
}

template <typename Predicate>
class FilterAdapter {
public:
    explicit FilterAdapter(Predicate pred)
        : pred_(std::move(pred))
    {}

    template <typename Flow>
    auto operator()(Flow&& flow)
    {
        struct FilteredFlow {
            FilteredFlow(Flow&& fl, Predicate pr)
                : flow_(std::forward<Flow>(fl))
                , pred_(std::move(pr))
            {}
            class Iterator {
            public:

                using InnerIterator = decltype(std::begin(std::declval<Flow>()));
                using iterator_category = std::forward_iterator_tag; 
                using value_type = decltype(*std::declval<InnerIterator>());
                using difference_type = std::ptrdiff_t;
                using pointer = std::remove_reference_t<value_type>*;
                using reference = value_type;

                Iterator(InnerIterator current, InnerIterator end, Predicate* pred)
                    : current_(current)
                    , end_(end)
                    , pred_(pred)
                {
                    skipInvalid();
                }

                bool operator!=(const Iterator& other) const {
                    return current_ != other.current_;
                }
                bool operator==(const Iterator& other) const {
                    return !(*this != other);
                }

                reference operator*() const {
                    return *current_;
                }
                pointer operator->() const {
                    return &(*current_);
                }

                Iterator& operator++() {
                    ++current_;
                    skipInvalid();
                    return *this;
                }

            private:
                void skipInvalid() {
                    while (current_ != end_ && !(*pred_)(*current_)) {
                        ++current_;
                    }
                }

                InnerIterator current_;
                InnerIterator end_;
                Predicate* pred_;
            };

            auto begin() {
                return Iterator(std::begin(flow_), std::end(flow_), &pred_);
            }
            auto end() {
                return Iterator(std::end(flow_), std::end(flow_), &pred_);
            }

            auto begin() const {
                return Iterator(std::begin(flow_), std::end(flow_), const_cast<Predicate*>(&pred_));
            }
            auto end()   const {
                return Iterator(std::end(flow_), std::end(flow_), const_cast<Predicate*>(&pred_));
            }

        private:
            Flow flow_;
            Predicate pred_;
        };

        return FilteredFlow(std::forward<Flow>(flow), pred_);
    }

private:
    Predicate pred_;
};

template <typename Predicate>
auto Filter(Predicate pred) {
    return FilterAdapter<Predicate>(std::move(pred));
}


class DropNulloptAdapter {
    public:
        template <typename Flow>
        auto operator()(Flow&& flow)
        {
            using OptionalType = decltype(*std::begin(flow)); 
            using ValueType = typename std::decay_t<OptionalType>::value_type; 
    
            struct DropNulloptFlow {
                DropNulloptFlow(Flow&& fl)
                    : flow_(std::forward<Flow>(fl))
                {}
    
                class Iterator {
                public:
                    using InnerIterator = decltype(std::begin(std::declval<Flow>()));
                    using optional_type = std::decay_t<OptionalType>;
                    using iterator_category = std::forward_iterator_tag; 
                    using value_type = typename optional_type::value_type; 
                    using difference_type = std::ptrdiff_t;
                    using reference = value_type&;
                    using pointer = value_type*;
    
                    Iterator(InnerIterator current, InnerIterator end)
                        : current_(current), end_(end)
                    {
                        skipNullopt();
                    }
    
                    bool operator!=(const Iterator& other) const {
                        return current_ != other.current_;
                    }
                    bool operator==(const Iterator& other) const {
                        return !(*this != other);
                    }
    
                    reference operator*() const {
                        return current_->value();
                    }
                    pointer operator->() const {
                        return std::addressof(current_->value());
                    }
    
                    Iterator& operator++() {
                        ++current_;
                        skipNullopt();
                        return *this;
                    }
    
                private:
                    void skipNullopt() {
                        while (current_ != end_ && !current_->has_value()) {
                            ++current_;
                        }
                    }
    
                    InnerIterator current_;
                    InnerIterator end_;
                };
    
                auto begin() {
                    return Iterator(std::begin(flow_), std::end(flow_));
                }
                auto end() {
                    return Iterator(std::end(flow_), std::end(flow_));
                }
    
                auto begin() const {
                    return Iterator(std::begin(flow_), std::end(flow_));
                }
                auto end()   const {
                    return Iterator(std::end(flow_), std::end(flow_));
                }
    
                Flow flow_;
            };
    
            return DropNulloptFlow(std::forward<Flow>(flow));
        }
    };
    
    inline DropNulloptAdapter DropNullopt()
    {
        return DropNulloptAdapter{};
    }


class WriteAdapter {
    public:
        WriteAdapter(std::ostream& os, char delimiter)
            : os_(os)
            , delimiter_(1, delimiter)
        {}
    

        WriteAdapter(std::ostream& os, std::string delimiter)
            : os_(os)
            , delimiter_(std::move(delimiter))
        {}
    
        template <typename Flow>
        Flow operator()(Flow&& flow) const {
            for (auto&& item : flow) {
                os_ << item << delimiter_;
            }
            return std::forward<Flow>(flow);
        }
    
    private:
        std::ostream& os_;  
        std::string delimiter_;
    };
    
    inline WriteAdapter Write(std::ostream& os, char delimiter) {
        return WriteAdapter(os, delimiter);
    }

    

template <typename RightFlow>
class JoinAdapterKV {
public:
    explicit JoinAdapterKV(RightFlow&& rightFlow)
        : rightFlow_(std::forward<RightFlow>(rightFlow))
    {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& leftFlow)
    {
        using LeftItem  = decltype(*std::begin(leftFlow));
        using RightItem = decltype(*std::begin(rightFlow_));

        using KeyType  = std::decay_t<decltype(std::declval<LeftItem>().key)>;
        using LValType = std::decay_t<decltype(std::declval<LeftItem>().value)>;
        using RValType = std::decay_t<decltype(std::declval<RightItem>().value)>;

        std::unordered_map<KeyType, RValType> rightMap;
        for (auto&& r : rightFlow_) {
            rightMap[r.key] = r.value;
        }

        using JR = JoinResult<LValType, RValType>;

        std::vector<JR> results;
        auto dist = std::distance(std::begin(leftFlow), std::end(leftFlow));
        if (dist > 0) {
            results.reserve(static_cast<std::size_t>(dist));
        }

        for (auto&& l : leftFlow) {
            auto it = rightMap.find(l.key);
            if (it != rightMap.end()) {
                results.push_back(JR{l.value, it->second});
            } else {
                results.push_back(JR{l.value, std::nullopt});
            }
        }

        struct JoinFlow {
            explicit JoinFlow(std::vector<JR>&& data)
                : data_(std::move(data))
            {}

            auto begin() { return data_.begin(); }
            auto end()   { return data_.end();   }
            auto begin() const { return data_.begin(); }
            auto end()   const { return data_.end();   }

            std::vector<JR> data_;
        };

        return JoinFlow(std::move(results));
    }

private:
    RightFlow rightFlow_;
};
    
template <typename RightFlow, typename LeftKeyGetter, typename RightKeyGetter>
class JoinAdapterGetters {
public:
    JoinAdapterGetters(RightFlow&& rf, LeftKeyGetter lkg, RightKeyGetter rkg)
        : rightFlow_(std::forward<RightFlow>(rf))
        , leftKeyGetter_(std::move(lkg))
        , rightKeyGetter_(std::move(rkg))
    {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& leftFlow)
    {
        using LeftItem = decltype(*std::begin(leftFlow));
        using RightItem = decltype(*std::begin(rightFlow_));

        using LKey = std::decay_t<decltype(leftKeyGetter_(std::declval<LeftItem>()))>;
        using RKey = std::decay_t<decltype(rightKeyGetter_(std::declval<RightItem>()))>;

        using LType = std::decay_t<LeftItem>;
        using RType = std::decay_t<RightItem>;

        std::unordered_map<RKey, RType> rightMap;
        for (auto&& r : rightFlow_) {
            auto key = rightKeyGetter_(r);
            rightMap[key] = r; 
        }

        using JR = JoinResult<LType, RType>;

        std::vector<JR> results;
        auto dist = std::distance(std::begin(leftFlow), std::end(leftFlow));
        if (dist > 0) {
            results.reserve(static_cast<std::size_t>(dist));
        }

        for (auto&& l : leftFlow) {
            auto lkey = leftKeyGetter_(l);
            auto it = rightMap.find(lkey);
            if (it != rightMap.end()) {
                results.push_back(JR{l, it->second});
            } else {
                results.push_back(JR{l, std::nullopt});
            }
        }

        struct JoinFlow {
            explicit JoinFlow(std::vector<JR>&& data)
                : data_(std::move(data))
            {}

            auto begin() { return data_.begin(); }
            auto end()   { return data_.end();   }
            auto begin() const { return data_.begin(); }
            auto end()   const { return data_.end();   }

            std::vector<JR> data_;
        };

        return JoinFlow(std::move(results));
    }

private:
    RightFlow rightFlow_;
    LeftKeyGetter leftKeyGetter_;
    RightKeyGetter rightKeyGetter_;
};

template <typename RightFlow>
auto Join(RightFlow&& rightFlow) {
    return JoinAdapterKV<RightFlow>(std::forward<RightFlow>(rightFlow));
}

template <typename RightFlow, typename LeftKeyGetter, typename RightKeyGetter>
auto Join(RightFlow&& rightFlow, LeftKeyGetter leftKeyGetter, RightKeyGetter rightKeyGetter) {
    return JoinAdapterGetters<RightFlow, LeftKeyGetter, RightKeyGetter>(
        std::forward<RightFlow>(rightFlow),
        std::move(leftKeyGetter),
        std::move(rightKeyGetter)
    );
}


class SplitAdapter {
    public:
        explicit SplitAdapter(std::string delimiters)
            : delimiters_(std::move(delimiters))
        {}
    
        template <typename Flow>
        auto operator()(Flow&& flow)
        {
            struct SplitFlow {
                using value_type = std::string;
    
                Flow flow_;
                std::string delimiters_;
    
                SplitFlow(Flow&& f, std::string d)
                    : flow_(std::forward<Flow>(f))
                    , delimiters_(std::move(d))
                {}
    
                class Iterator {
                public:
                    using iterator_category = std::forward_iterator_tag;
                    using value_type = std::string;
                    using difference_type = std::ptrdiff_t;
                    using pointer = value_type*;
                    using reference = value_type&;
    
                    Iterator()
                        : outerCurrent_{}, outerEnd_{}, valid_(false)
                    {}
    
                    Iterator(decltype(std::begin(std::declval<Flow>())) oc,
                             decltype(std::end(std::declval<Flow>())) oe,
                             const std::string* delims)
                        : outerCurrent_(oc)
                        , outerEnd_(oe)
                        , delimiters_(delims)
                        , valid_(true)
                    {
                        if (outerCurrent_ != outerEnd_) {
                            currentStr_ = outerCurrent_->str();
                            strPos_ = 0;
                        } else {
                            valid_ = false;
                        }
                        nextToken();
                    }
    
                    bool operator==(const Iterator& other) const {
                        if (!valid_ && !other.valid_) {
                            return true;
                        }
                        if (!valid_ || !other.valid_) {
                            return false;
                        }
                        return (outerCurrent_ == other.outerCurrent_) && (token_ == other.token_);
                    }
                    bool operator!=(const Iterator& other) const {
                        return !(*this == other);
                    }
    
                    const std::string& operator*() const {
                        return token_;
                    }
                    const std::string* operator->() const {
                        return &token_;
                    }
    
                    Iterator& operator++() {
                        nextToken();
                        return *this;
                    }
    
                private:
                    void nextToken() {
                        while (valid_) {
                            if (outerCurrent_ == outerEnd_) {
                                valid_ = false;
                                return;
                            }
    
                            if (strPos_ >= currentStr_.size()) {
                                ++outerCurrent_;
                                if (outerCurrent_ == outerEnd_) {
                                    valid_ = false;
                                    return;
                                }
                                currentStr_ = outerCurrent_->str();
                                strPos_ = 0;
                                continue;
                            }
    
                            size_t startPos = strPos_;
                            size_t endPos   = findNextDelimiter(startPos);
    
                            token_ = currentStr_.substr(startPos, endPos - startPos);
    
                            if (endPos == std::string::npos) {
                                strPos_ = currentStr_.size();
                            } else {
                                strPos_ = endPos + 1;
                            }
    
                            return;
                        }
                    }
    
                    size_t findNextDelimiter(size_t pos) const {
                        for (size_t i = pos; i < currentStr_.size(); i++) {
                            if (isDelimiter(currentStr_[i])) {
                                return i;
                            }
                        }
                        return std::string::npos;
                    }
    
                    bool isDelimiter(char c) const {
                        return (delimiters_->find(c) != std::string::npos);
                    }
    
                    decltype(std::begin(std::declval<Flow>())) outerCurrent_;
                    decltype(std::end(std::declval<Flow>()))   outerEnd_;
    
                    std::string currentStr_;
                    size_t strPos_ = 0;
    
                    const std::string* delimiters_;
    
                    std::string token_;
    
                    bool valid_;
                };
    
                auto begin() {
                    return Iterator(std::begin(flow_), std::end(flow_), &delimiters_);
                }
                auto end() {
                    return Iterator();
                }
    
                auto begin() const {
                    return Iterator(std::begin(flow_), std::end(flow_), &delimiters_);
                }
                auto end() const {
                    return Iterator();
                }
            };
    
            return SplitFlow(std::forward<Flow>(flow), std::move(delimiters_));
        }
    
    private:
        std::string delimiters_;
    };
    
    inline SplitAdapter Split(std::string delimiters)
    {
        return SplitAdapter(std::move(delimiters));
    }

template <typename Func>
class TransformAdapter {
public:
    explicit TransformAdapter(Func func)
        : func_(std::move(func))
    {}
    template <typename Flow>
    auto operator()(Flow&& flow)
    {

        struct TransformFlow {
            using FlowType = std::decay_t<Flow>;
            using FuncType = std::decay_t<Func>;

            FlowType flow_;
            FuncType func_;
            class Iterator {
            public:
                using InnerIter = decltype(std::begin(std::declval<FlowType&>()));
                using iterator_category = std::forward_iterator_tag;
                using InnerValueType = decltype(*std::declval<InnerIter>());
                using ResultType = decltype(std::declval<FuncType>()(std::declval<InnerValueType>()));

                using value_type = std::decay_t<ResultType>;
                using difference_type = std::ptrdiff_t;
                using pointer = value_type*;
                using reference  = value_type;

                Iterator() = default;

                Iterator(InnerIter current,
                         InnerIter end,
                         const FuncType* funcPtr)
                    : current_(current)
                    , end_(end)
                    , funcPtr_(funcPtr)
                {}

                bool operator!=(const Iterator& other) const {
                    return current_ != other.current_;
                }
                bool operator==(const Iterator& other) const {
                    return !(*this != other);
                }
                value_type operator*() const {
                    auto&& element = *current_;
                    return (*funcPtr_)(element);
                }

                Iterator& operator++() {
                    ++current_;
                    return *this;
                }

            private:
                InnerIter current_;
                InnerIter end_;
                const FuncType* funcPtr_ = nullptr;
            };

            auto begin() {
                return Iterator(std::begin(flow_), std::end(flow_), &func_);
            }
            auto end() {
                return Iterator(std::end(flow_), std::end(flow_), &func_);
            }

            auto begin() const {
                return Iterator(std::begin(flow_), std::end(flow_), &func_);
            }
            auto end() const {
                return Iterator(std::end(flow_), std::end(flow_), &func_);
            }
        };

        return TransformFlow{
            std::forward<Flow>(flow),
            std::move(func_)
        };
    }

private:
    Func func_;
};

template <typename Func>
auto Transform(Func func) {
    return TransformAdapter<Func>(std::move(func));
}



template <typename ParserFunc>
class SplitExpectedAdapter {
public:
    explicit SplitExpectedAdapter(ParserFunc) {

    }

    template <typename Flow>
    auto operator()(Flow&& flow)
    {
        using ItemType = decltype(*std::begin(flow));
        using TType = typename std::decay_t<ItemType>::value_type;
        using EType = typename std::decay_t<ItemType>::error_type;

        std::vector<EType> errors;
        std::vector<TType> oks;

        for (auto&& item : flow) {
            if (item.has_value()) {
                oks.push_back(item.value());
            } else {
                errors.push_back(item.error());
            }
        }

        struct UnexpectedFlow {
            std::vector<EType> data;
            auto begin() { return data.begin(); }
            auto end()   { return data.end();   }
            auto begin() const { return data.begin(); }
            auto end()   const { return data.end();   }
        };

        struct GoodFlow {
            std::vector<TType> data;
            auto begin() { return data.begin(); }
            auto end()   { return data.end();   }
            auto begin() const { return data.begin(); }
            auto end()   const { return data.end();   }
        };

        UnexpectedFlow uf{std::move(errors)};
        GoodFlow gf{std::move(oks)};
        return std::make_pair(std::move(uf), std::move(gf));
    }
};

template <typename ParserFunc>
auto SplitExpected(ParserFunc func) {
    return SplitExpectedAdapter<ParserFunc>(std::move(func));
}

class OpenFilesAdapter {
public:
    OpenFilesAdapter() = default;

    template <typename Flow>
    auto operator()(Flow&& flow)
    {
        struct OpenFilesFlow {
            using FlowType = std::decay_t<Flow>;
            FlowType flow_;

            explicit OpenFilesFlow(FlowType f)
                : flow_(std::move(f))
            {}

            class Iterator {
            public:
                using InnerIter = decltype(std::begin(std::declval<FlowType&>()));
                using iterator_category = std::forward_iterator_tag;
                using value_type = std::ifstream;
                using difference_type = std::ptrdiff_t;

                Iterator() = default;

                Iterator(InnerIter current, InnerIter end)
                    : current_(current)
                    , end_(end)
                {
                    if (current_ != end_) {
                        openFile();
                    }
                }

                bool operator==(const Iterator& other) const {
                    return current_ == other.current_;
                }
                bool operator!=(const Iterator& other) const {
                    return !(*this == other);
                }
                std::ifstream& operator*() {
                    return stream_;
                }
                std::ifstream* operator->() {
                    return &stream_;
                }

                Iterator& operator++() {
                    ++current_;
                    if (current_ != end_) {
                        stream_.close();
                        openFile();
                    } else {
                        stream_.close();
                    }
                    return *this;
                }

            private:
                void openFile() {
                    if (current_ != end_) {
                        const auto& path = *current_;
                        stream_.open(path, std::ios::in | std::ios::binary);
                    }
                }

                InnerIter current_, end_;
                std::ifstream stream_;
            };

            auto begin() { 
                return Iterator(std::begin(flow_), std::end(flow_));
            }
            auto end() {
                return Iterator(std::end(flow_), std::end(flow_));
            }
        };

        return OpenFilesFlow(std::forward<Flow>(flow));
    }
};

inline auto OpenFiles() {
    return OpenFilesAdapter{};
}


class OutAdapter {
public:
    explicit OutAdapter(std::ostream& os)
        : os_(os)
    {}
    template <typename Flow>
    Flow operator()(Flow&& flow) const {
        for (auto&& item : flow) {
            os_ << item << " "; 
        }
        return std::forward<Flow>(flow);
    }

private:
    std::ostream& os_;
};

inline auto Out(std::ostream& os)
{
    return OutAdapter(os);
}


class RecDirFlow {
public:
    explicit RecDirFlow(std::filesystem::path root)
        : recIter_(std::move(root))
        , end_()
    {}
    class Iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type        = std::filesystem::path;
        using difference_type   = std::ptrdiff_t;
        using reference         = value_type;
        using pointer           = const value_type*;

        Iterator() = default;

        explicit Iterator(std::filesystem::recursive_directory_iterator it)
            : current_(it)
        {}

        bool operator==(const Iterator& other) const {
            return current_ == other.current_;
        }
        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

        value_type operator*() const {
            return current_->path();
        }

        Iterator& operator++() {
            ++current_;
            return *this;
        }

    private:
        std::filesystem::recursive_directory_iterator current_;
    };

    Iterator begin() const {
        return Iterator(recIter_);
    }
    Iterator end() const {
        return Iterator(end_);
    }

private:
    std::filesystem::recursive_directory_iterator recIter_;
    std::filesystem::recursive_directory_iterator end_;
};

inline RecDirFlow Dir(const std::filesystem::path& root) {
    return RecDirFlow(root);
}

}
