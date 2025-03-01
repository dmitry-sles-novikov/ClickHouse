#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>


namespace DB
{

using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;
using FunctionLess = FunctionComparison<LessOp, NameLess>;
extern template class FunctionComparison<LessOp, NameLess>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
extern template class FunctionComparison<EqualsOp, NameEquals>;

REGISTER_FUNCTION(LessOrEquals)
{
    factory.registerFunction<FunctionLessOrEquals>();
}

template <>
ColumnPtr FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(
    const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count) const
{
    FunctionOverloadResolverPtr less_or_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionLessOrEquals>(params));

    FunctionOverloadResolverPtr less
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionLess>(params));

    FunctionOverloadResolverPtr func_builder_or
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    FunctionOverloadResolverPtr func_builder_and
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(params));

    return executeTupleLessGreaterImpl(
        less,
        less_or_equals,
        func_builder_and,
        func_builder_or,
        func_builder_equals,
        x, y, tuple_size, input_rows_count);
}

}
